/*
 * Copyright 2025 RAW Labs S.A.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0, included in the file
 * licenses/APL.txt.
 */

package com.rawlabs.das.sqlite

import java.sql.{Connection, ResultSet, SQLException}
import javax.sql.DataSource

import scala.util.{Try, Using}

import com.fasterxml.jackson.databind.node.{NullNode, TextNode}
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.google.protobuf.ByteString
import com.rawlabs.das.sdk.DASExecuteResult
import com.rawlabs.das.sdk.scala.DASTable.TableEstimate
import com.rawlabs.protocol.das.v1.tables._
import com.rawlabs.protocol.das.v1.types._
import com.typesafe.scalalogging.StrictLogging

/**
 * A DAS backend for SQLite, providing:
 *   - Table discovery (`tables()`)
 *   - Basic SQL query execution (`execute(query)`)
 *   - Query cost estimates (very naive in SQLite) (`estimate(query)`)
 *   - Batch query execution with parameters (`batchQuery(...)`)
 *   - (Optional) returning-rows execution (`executeReturningRows(...)`) if using SQLite >= 3.35 that supports RETURNING
 *
 * @param dataSource A pre-configured DataSource (e.g., HikariCP for SQLite JDBC URL)
 */
private class DASSqliteBackend(dataSource: DataSource) extends StrictLogging {

  // ------------------------------------------------------------------------------------------------------
  // ObjectMapper for JSON
  // ------------------------------------------------------------------------------------------------------
  private val mapper = new ObjectMapper()
  private val nodeFactory = mapper.getNodeFactory

  // ------------------------------------------------------------------------------------------------------
  // Public API
  // ------------------------------------------------------------------------------------------------------

  /**
   * Returns a map of tableName -> DASSqliteTable, each containing a TableDefinition. Enumerates all user-defined tables
   * in the SQLite database, skipping internal tables named `sqlite_*`.
   *
   * @throws SQLException if there's an error querying the DB
   */
  def tables(): Map[String, DASSqliteTable] = {
    logger.debug("Fetching tables from the SQLite database")

    Using
      .Manager { use =>
        val conn = use(dataSource.getConnection)

        // Retrieve the list of tables from sqlite_master
        val listTablesSQL =
          """
            |SELECT name
            |FROM sqlite_master
            |WHERE type='table'
            |  AND name NOT LIKE 'sqlite_%'
            |ORDER BY name
          """.stripMargin

        val stmt = use(conn.createStatement())
        val rs = use(stmt.executeQuery(listTablesSQL))

        val tableMap = scala.collection.mutable.Map.empty[String, DASSqliteTable]

        while (rs.next()) {
          val tableName = rs.getString("name").toLowerCase
          val pkOpt = primaryKey(conn, tableName)
          val tdef = tableDefinition(conn, tableName)
          tableMap += tableName -> new DASSqliteTable(this, tdef, pkOpt)
        }

        tableMap.toMap
      }
      .recover { case ex: SQLException =>
        logger.error(s"Error while fetching tables: ${ex.getMessage}", ex)
        throw ex
      }
      .get
  }

  /**
   * Executes a SQL query and returns a DASExecuteResult that streams rows from the ResultSet. Caller is responsible for
   * consuming the result and eventually calling `.close()` on it.
   *
   * @param query SQL query to execute
   * @return DASExecuteResult that allows row-by-row iteration
   * @throws SQLException if there's a DB error
   */
  def execute(query: String): DASExecuteResult = {
    logger.debug(s"Preparing to execute query: $query")
    val conn = dataSource.getConnection
    try {
      val stmt = conn.createStatement()
      val rs = stmt.executeQuery(query)

      // Return a DASExecuteResult with manual streaming
      new DASExecuteResult {
        private var nextFetched = false
        private var hasMore = false

        override def close(): Unit = {
          Try(rs.close()).failed.foreach { t =>
            logger.warn("Failed to close ResultSet cleanly", t)
          }
          Try(stmt.close()).failed.foreach { t =>
            logger.warn("Failed to close Statement cleanly", t)
          }
          Try(conn.close()).failed.foreach { t =>
            logger.warn("Failed to close Connection cleanly", t)
          }
        }

        override def hasNext: Boolean = {
          if (!nextFetched) {
            hasMore = rs.next()
            nextFetched = true
          }
          hasMore
        }

        override def next(): Row = {
          if (!hasNext) throw new NoSuchElementException("No more rows available.")
          nextFetched = false

          val rowBuilder = Row.newBuilder()
          val meta = rs.getMetaData
          val colCount = meta.getColumnCount

          for (i <- 1 to colCount) {
            val colName = meta.getColumnName(i).toLowerCase
            val value = toDASValue(rs, i)
            rowBuilder.addColumns(Column.newBuilder().setName(colName).setData(value).build())
          }
          rowBuilder.build()
        }
      }
    } catch {
      case ex: SQLException =>
        logger.error(s"Error executing query '$query': ${ex.getMessage}", ex)
        // Make sure to close the connection if we fail before returning the result
        Try(conn.close()).failed.foreach { t =>
          logger.warn("Failed to close Connection in error scenario", t)
        }
        throw ex
    }
  }

  /**
   * Returns an estimate of the row count and average row width for the given query. Note: SQLite doesn't provide
   * straightforward estimates via EXPLAIN (or EXPLAIN QUERY PLAN). We fallback to a naive default (100 rows @ 100
   * width).
   *
   * @param query SQL query to estimate
   * @return TableEstimate(rowCount, avgRowWidthInBytes)
   * @throws SQLException if there's a DB error
   */
  def estimate(query: String): TableEstimate = {
    logger.debug(s"Estimating cost for query (very naive in SQLite): $query")

    // Attempt a rudimentary parse from EXPLAIN QUERY PLAN or fallback
    val explainSql = s"EXPLAIN QUERY PLAN $query"

    Using
      .Manager { use =>
        val conn = use(dataSource.getConnection)
        val stmt = use(conn.createStatement())
        val rs = use(stmt.executeQuery(explainSql))

        // In SQLite, EXPLAIN QUERY PLAN returns lines describing the plan, but no direct row/width stats
        // We'll just do a naive fallback approach:
        if (!rs.next()) {
          logger.warn("EXPLAIN QUERY PLAN returned no rows; using default estimate (100 rows @ 100 width).")
          TableEstimate(100, 100)
        } else {
          // We at least return some naive defaults
          TableEstimate(100, 100)
        }
      }
      .recover { case ex: SQLException =>
        logger.error(s"Error while estimating query '$query': ${ex.getMessage}", ex)
        throw ex
      }
      .get
  }

  /**
   * Executes a batch query (e.g. INSERT/UPDATE with parameters) and returns an array of update counts for each batch
   * item.
   *
   * @param query SQL query with placeholders (e.g. "INSERT INTO ... VALUES (?, ?)")
   * @param batchParameters Sequence of parameter sequences, one per batch
   * @return array of update counts
   * @throws SQLException if there's a DB error
   */
  def batchQuery(query: String, batchParameters: Seq[Seq[Value]]): Array[Int] = {
    logger.debug(s"Preparing to execute batch: $query (with ${batchParameters.size} batches)")

    Using
      .Manager { use =>
        val conn = use(dataSource.getConnection)
        val ps = use(conn.prepareStatement(query))

        // Some SQLite drivers may return "UNKNOWN" or fail on param metadata, so we handle gracefully
        val meta = ps.getParameterMetaData

        for (params <- batchParameters) {
          params.zipWithIndex.foreach { case (dasValue, idx) =>
            val paramTypeName =
              try {
                meta.getParameterTypeName(idx + 1)
              } catch {
                case _: SQLException => "UNKNOWN"
              }
            ps.setObject(idx + 1, convertDASValueToJdbcValue(dasValue, paramTypeName, conn))
          }
          ps.addBatch()
        }
        ps.executeBatch()
      }
      .recover { case ex: SQLException =>
        logger.error(s"Error in batch execution for query '$query': ${ex.getMessage}", ex)
        throw ex
      }
      .get
  }

  /**
   * Executes a parameterized INSERT/UPDATE/DELETE that returns rows via `RETURNING *` (SQLite 3.35+ only). If the
   * SQLite version does not support RETURNING, this may fail.
   *
   * @param sql The SQL statement containing placeholders, e.g. "INSERT INTO mytable (col1, col2) VALUES (?, ?)
   *   RETURNING *"
   * @param params The flattened list of parameter values in the correct order
   * @return a DASExecuteResult that can be used to iterate over returned rows
   */
  def executeReturningRows(sql: String, params: Seq[Value]): DASExecuteResult = {
    logger.debug(s"Executing returning-rows query: $sql with ${params.size} params (SQLite 3.35+ feature)")

    val conn = dataSource.getConnection
    try {
      val ps = conn.prepareStatement(sql)
      val meta = ps.getParameterMetaData

      // Bind each param in order
      params.zipWithIndex.foreach { case (dasValue, idx) =>
        val paramTypeName =
          try {
            meta.getParameterTypeName(idx + 1)
          } catch {
            case _: SQLException => "UNKNOWN"
          }
        ps.setObject(idx + 1, convertDASValueToJdbcValue(dasValue, paramTypeName, conn))
      }

      // Execute the statement and get a ResultSet
      val rs = ps.executeQuery()

      new DASExecuteResult {
        private var nextFetched = false
        private var hasMore = false

        override def close(): Unit = {
          scala.util.Try(rs.close()).failed.foreach { t =>
            logger.warn("Failed to close ResultSet cleanly", t)
          }
          scala.util.Try(ps.close()).failed.foreach { t =>
            logger.warn("Failed to close PreparedStatement cleanly", t)
          }
          scala.util.Try(conn.close()).failed.foreach { t =>
            logger.warn("Failed to close Connection cleanly", t)
          }
        }

        override def hasNext: Boolean = {
          if (!nextFetched) {
            hasMore = rs.next()
            nextFetched = true
          }
          hasMore
        }

        override def next(): Row = {
          if (!hasNext) throw new NoSuchElementException("No more rows returned.")
          nextFetched = false

          val rowBuilder = Row.newBuilder()
          val meta = rs.getMetaData
          val colCount = meta.getColumnCount
          for (i <- 1 to colCount) {
            val colName = meta.getColumnName(i).toLowerCase
            val value = toDASValue(rs, i)
            rowBuilder.addColumns(
              Column
                .newBuilder()
                .setName(colName)
                .setData(value)
                .build())
          }
          rowBuilder.build()
        }
      }
    } catch {
      case ex: SQLException =>
        logger.error(s"Error in executeReturningRows for query '$sql': ${ex.getMessage}", ex)
        scala.util.Try(conn.close()) // best effort
        throw ex
    }
  }

  // ------------------------------------------------------------------------------------------------------
  // Private Helpers for Table definitions, PK extraction
  // ------------------------------------------------------------------------------------------------------

  /**
   * Retrieves exactly one primary key column name for the given table (if it has exactly one PK column). Uses "PRAGMA
   * table_info(tableName)", which returns: cid, name, type, notnull, dflt_value, pk
   */
  private def primaryKey(conn: Connection, tableName: String): Option[String] = {
    val pragmaSQL = s"PRAGMA table_info('$tableName')"

    Using.resource(conn.createStatement()) { stmt =>
      Using.resource(stmt.executeQuery(pragmaSQL)) { rs =>
        val pkColumns = scala.collection.mutable.ListBuffer.empty[String]
        while (rs.next()) {
          val pk = rs.getInt("pk") // 1 means part of PK, 0 means not
          if (pk == 1) {
            pkColumns += rs.getString("name")
          }
        }
        if (pkColumns.size == 1) Some(pkColumns.head.toLowerCase)
        else None
      }
    }
  }

  /**
   * Builds a TableDefinition for the given table by inspecting PRAGMA table_info.
   */
  private def tableDefinition(conn: Connection, tableName: String): TableDefinition = {
    val descriptionBuilder = TableDefinition
      .newBuilder()
      .setTableId(TableId.newBuilder().setName(tableName))
      .setDescription(s"Table '$tableName' in SQLite database")

    val pragmaSQL = s"PRAGMA table_info('$tableName')"

    Using.resource(conn.createStatement()) { stmt =>
      Using.resource(stmt.executeQuery(pragmaSQL)) { rs =>
        while (rs.next()) {
          val colName = rs.getString("name")
          val declaredType = rs.getString("type") // e.g. "INTEGER", "TEXT", "REAL", "BLOB", "NUMERIC", ...
          val notNull = rs.getInt("notnull") == 1

          toDASType(declaredType, !notNull) match {
            case Right(t) =>
              val colDef = ColumnDefinition.newBuilder().setName(colName).setType(t).build()
              descriptionBuilder.addColumns(colDef)
            case Left(err) =>
              logger.warn(s"Skipping column '$colName' in table '$tableName': $err")
          }
        }
      }
    }
    descriptionBuilder.build()
  }

  // ------------------------------------------------------------------------------------------------------
  // Private Conversion Helpers: SQLite -> DAS Value
  // ------------------------------------------------------------------------------------------------------

  /**
   * Convert the column at index `i` in the ResultSet into a DAS Value. We rely on either
   * `getMetaData.getColumnTypeName(i)` or direct `rs.getObject(i)` checks to map to DAS. We also attempt to handle some
   * additional declared type keywords (BOOLEAN, DATE, DATETIME, etc.) if present.
   */
  private def toDASValue(rs: ResultSet, i: Int): Value = {
    val jdbcTypeName =
      try {
        rs.getMetaData.getColumnTypeName(i).toLowerCase
      } catch {
        // Some JDBC drivers for SQLite might not return a meaningful type name
        case _: SQLException => "unknown"
      }

    val obj = rs.getObject(i)
    val builder = Value.newBuilder()

    if (obj == null) {
      builder.setNull(ValueNull.newBuilder())
      return builder.build()
    }

    // We'll do best-effort based on common SQLite type affinities and extended keywords
    //   - "INT" or "INTEGER"
    //   - "REAL", "FLOAT", "DOUBLE"
    //   - "BLOB"
    //   - "TEXT", "CHAR", "CLOB"
    //   - "NUMERIC", "DECIMAL"
    //   - "BOOLEAN"
    //   - "DATE", "TIME", "DATETIME", "TIMESTAMP" (heuristic handling)
    //
    // If none match, we fallback to object-based detection.

    jdbcTypeName match {
      case t if t.contains("int") =>
        // Could be a normal integer or a long
        val longVal = rs.getLong(i)
        if (rs.wasNull()) {
          builder.setNull(ValueNull.newBuilder())
        } else {
          // if it fits in Int
          if (longVal >= Int.MinValue && longVal <= Int.MaxValue) {
            builder.setInt(ValueInt.newBuilder().setV(longVal.toInt))
          } else {
            builder.setLong(ValueLong.newBuilder().setV(longVal))
          }
        }

      case t if t.contains("real") || t.contains("floa") || t.contains("doub") =>
        val doubleVal = rs.getDouble(i)
        if (rs.wasNull()) {
          builder.setNull(ValueNull.newBuilder())
        } else {
          builder.setDouble(ValueDouble.newBuilder().setV(doubleVal))
        }

      case t if t.contains("blob") =>
        val bytes = rs.getBytes(i)
        builder.setBinary(ValueBinary.newBuilder().setV(ByteString.copyFrom(bytes)))

      case t if t.contains("text") || t.contains("char") || t.contains("clob") =>
        val strVal = rs.getString(i)
        builder.setString(ValueString.newBuilder().setV(strVal))

      case t if t.contains("boolean") =>
        // SQLite has no real boolean, but let's interpret 1=>true, 0=>false if numeric
        // or 'true'/'false' if string, etc.
        obj match {
          case boolVal: java.lang.Boolean =>
            builder.setBool(ValueBool.newBuilder().setV(boolVal))
          case num: java.lang.Number =>
            builder.setBool(ValueBool.newBuilder().setV(num.intValue() != 0))
          case s: String =>
            val lower = s.trim.toLowerCase
            val boolParsed = lower == "true" || lower == "1"
            builder.setBool(ValueBool.newBuilder().setV(boolParsed))
          case _ =>
            // fallback
            builder.setString(ValueString.newBuilder().setV(obj.toString))
        }

      case t if t.contains("numeric") || t.contains("dec") =>
        // This might be decimal or some numeric
        // We'll attempt getBigDecimal; if not, fallback to string
        val bd = Try(rs.getBigDecimal(i)).toOption
        bd match {
          case Some(numVal) =>
            builder.setDecimal(ValueDecimal.newBuilder().setV(numVal.toPlainString))
          case None =>
            // fallback
            val strVal = rs.getString(i)
            if (strVal == null) builder.setNull(ValueNull.newBuilder())
            else {
              // Attempt parse
              Try(new java.math.BigDecimal(strVal)).toOption match {
                case Some(parsed) =>
                  builder.setDecimal(ValueDecimal.newBuilder().setV(parsed.toPlainString))
                case None =>
                  builder.setString(ValueString.newBuilder().setV(strVal))
              }
            }
        }

      case t if t.contains("date") || t.contains("time") =>
        // Heuristic approach: let's see if the actual data in the column can parse as date/time
        // We'll attempt to parse as a timestamp, date, or time if it looks valid. Otherwise fallback to string.
        val strVal = rs.getString(i)
        if (strVal == null) {
          builder.setNull(ValueNull.newBuilder())
        } else {
          // Attempt multiple parse patterns
          val trimmed = strVal.trim
          // We'll do a simple check if it has a time portion
          if (trimmed.matches("\\d{4}-\\d{2}-\\d{2}[ T]\\d{2}:\\d{2}:\\d{2}.*")) {
            // Attempt parse as local date-time
            val maybeTs = parseAsTimestamp(trimmed)
            if (maybeTs.isDefined) {
              val ldt = maybeTs.get
              builder.setTimestamp(
                ValueTimestamp
                  .newBuilder()
                  .setYear(ldt.getYear)
                  .setMonth(ldt.getMonthValue)
                  .setDay(ldt.getDayOfMonth)
                  .setHour(ldt.getHour)
                  .setMinute(ldt.getMinute)
                  .setSecond(ldt.getSecond)
                  .setNano(ldt.getNano))
            } else {
              // fallback string
              builder.setString(ValueString.newBuilder().setV(strVal))
            }
          } else if (trimmed.matches("\\d{4}-\\d{2}-\\d{2}")) {
            // Attempt parse as date only
            val maybeDate = parseAsDate(trimmed)
            if (maybeDate.isDefined) {
              val ld = maybeDate.get
              builder.setDate(
                ValueDate.newBuilder().setYear(ld.getYear).setMonth(ld.getMonthValue).setDay(ld.getDayOfMonth))
            } else {
              builder.setString(ValueString.newBuilder().setV(strVal))
            }
          } else if (trimmed.matches("\\d{2}:\\d{2}:\\d{2}.*")) {
            // Attempt parse as time
            val maybeTime = parseAsTime(trimmed)
            if (maybeTime.isDefined) {
              val lt = maybeTime.get
              builder.setTime(
                ValueTime.newBuilder().setHour(lt.getHour).setMinute(lt.getMinute).setSecond(lt.getSecond))
            } else {
              builder.setString(ValueString.newBuilder().setV(strVal))
            }
          } else {
            // fallback to string
            builder.setString(ValueString.newBuilder().setV(strVal))
          }
        }

      case "unknown" =>
        // Some JDBC drivers do not return a column type name. We'll fallback to object-based detection
        convertFallbackObject(obj, rs, i, builder)

      case _ =>
        // Fallback if the declared type is not recognized
        convertFallbackObject(obj, rs, i, builder)
    }

    builder.build()
  }

  /**
   * Attempts to interpret an SQLite object in a fallback manner if the column type name isn't reliable.
   */
  private def convertFallbackObject(obj: Any, rs: ResultSet, i: Int, builder: Value.Builder): Unit = {
    obj match {
      case n: java.lang.Number =>
        // Could be int, long, double, etc.
        val d = n.doubleValue()
        val l = n.longValue()
        // if integral
        if (d == l.toDouble) {
          // within int range?
          if (l >= Int.MinValue && l <= Int.MaxValue) {
            builder.setInt(ValueInt.newBuilder().setV(l.toInt))
          } else {
            builder.setLong(ValueLong.newBuilder().setV(l))
          }
        } else {
          builder.setDouble(ValueDouble.newBuilder().setV(d))
        }
      case s: String =>
        builder.setString(ValueString.newBuilder().setV(s))
      case b: Array[Byte] =>
        builder.setBinary(ValueBinary.newBuilder().setV(ByteString.copyFrom(b)))
      case boolVal: java.lang.Boolean =>
        builder.setBool(ValueBool.newBuilder().setV(boolVal))
      case dateVal: java.sql.Date =>
        val ld = dateVal.toLocalDate
        builder.setDate(ValueDate.newBuilder().setYear(ld.getYear).setMonth(ld.getMonthValue).setDay(ld.getDayOfMonth))
      case timeVal: java.sql.Time =>
        val lt = timeVal.toLocalTime
        builder.setTime(ValueTime.newBuilder().setHour(lt.getHour).setMinute(lt.getMinute).setSecond(lt.getSecond))
      case tsVal: java.sql.Timestamp =>
        val ldt = tsVal.toLocalDateTime
        builder.setTimestamp(
          ValueTimestamp
            .newBuilder()
            .setYear(ldt.getYear)
            .setMonth(ldt.getMonthValue)
            .setDay(ldt.getDayOfMonth)
            .setHour(ldt.getHour)
            .setMinute(ldt.getMinute)
            .setSecond(ldt.getSecond)
            .setNano(ldt.getNano))
      case _ =>
        // fallback to string
        builder.setString(ValueString.newBuilder().setV(rs.getString(i)))
    }
  }

  import java.time.format.DateTimeFormatter
  // ------------------------------------------------------------------------------------------------------
  // Private Date/Time parse helpers
  // ------------------------------------------------------------------------------------------------------
  import java.time.{LocalDate, LocalDateTime, LocalTime}

  import scala.util.Try

  private val datePatterns = List("yyyy-MM-dd", "yyyy/MM/dd").map(DateTimeFormatter.ofPattern)

  private val dateTimePatterns = List(
    "yyyy-MM-dd HH:mm:ss",
    "yyyy-MM-dd HH:mm:ss.SSS",
    "yyyy/MM/dd HH:mm:ss",
    "yyyy/MM/dd HH:mm:ss.SSS",
    "yyyy-MM-dd'T'HH:mm:ss",
    "yyyy-MM-dd'T'HH:mm:ss.SSS").map(DateTimeFormatter.ofPattern)

  private val timePatterns = List("HH:mm:ss", "HH:mm:ss.SSS").map(DateTimeFormatter.ofPattern)

  private def parseAsDate(str: String): Option[LocalDate] = {
    datePatterns.view
      .map(p => Try(LocalDate.parse(str, p)).toOption)
      .collectFirst { case Some(d) => d }
  }

  private def parseAsTimestamp(str: String): Option[LocalDateTime] = {
    dateTimePatterns.view
      .map(p => Try(LocalDateTime.parse(str, p)).toOption)
      .collectFirst { case Some(dt) => dt }
  }

  private def parseAsTime(str: String): Option[LocalTime] = {
    timePatterns.view
      .map(p => Try(LocalTime.parse(str, p)).toOption)
      .collectFirst { case Some(t) => t }
  }

  // ------------------------------------------------------------------------------------------------------
  // Private Conversion Helpers: DAS Value -> JDBC
  // ------------------------------------------------------------------------------------------------------

  /**
   * Converts a DAS Value into a JDBC-compatible value, guided by the (possibly unreliable) SQLite column type name
   * `colType`.
   */
  private def convertDASValueToJdbcValue(value: Value, colType: String, conn: Connection): Any = {
    val lowerType = Option(colType).map(_.toLowerCase).getOrElse("unknown")

    // If it's NULL in DAS, it's NULL to JDBC
    if (value.hasNull) {
      return null
    }

    // In SQLite, columns are dynamically typed. We'll do best-effort:
    //   - If it's integer/long/short, store as a numeric
    //   - If it's double/float/decimal, store as a real or numeric
    //   - If it's string, store as text
    //   - If it's bool, we might store as 1 or 0 if declared "BOOLEAN"
    //   - If it's date/time/timestamp, we store as string in ISO form unless we detect an integer column
    //   - Binaries store as blob
    //   - Otherwise fallback to a JSON string representation
    lowerType match {
      case t if t.contains("int") =>
        // Store numeric
        fallbackToNumber(value)

      case t if t.contains("real") || t.contains("floa") || t.contains("doub") =>
        // Store floating
        if (value.hasDouble) value.getDouble.getV
        else if (value.hasFloat) value.getFloat.getV.toDouble
        else if (value.hasDecimal) Try(new java.math.BigDecimal(value.getDecimal.getV).doubleValue()).getOrElse(0.0)
        else fallbackToNumber(value)

      case t if t.contains("blob") =>
        // Store as blob
        fallbackToBytes(value)

      case t if t.contains("boolean") =>
        // We store true => 1, false => 0
        if (value.hasBool) {
          if (value.getBool.getV) 1 else 0
        } else {
          // fallback numeric
          fallbackToNumber(value)
        }

      case t if t.contains("numeric") || t.contains("dec") =>
        // Attempt BigDecimal
        if (value.hasDecimal) new java.math.BigDecimal(value.getDecimal.getV)
        else fallbackToNumber(value)

      case t
          if (t.contains("text") || t.contains("char") || t.contains("clob")) || t.contains("date") || t.contains(
            "time") =>
        // We'll store as string representation
        fallbackToString(value)

      case "unknown" =>
        // fallback approach
        fallbackToString(value)

      case _ =>
        // fallback approach
        fallbackToString(value)
    }
  }

  private def fallbackToNumber(value: Value): Any = {
    if (value.hasLong) {
      value.getLong.getV
    } else if (value.hasInt) {
      value.getInt.getV
    } else if (value.hasShort) {
      value.getShort.getV.toInt
    } else if (value.hasDouble) {
      value.getDouble.getV
    } else if (value.hasFloat) {
      value.getFloat.getV.toDouble
    } else if (value.hasDecimal) {
      // Try returning a BigDecimal
      new java.math.BigDecimal(value.getDecimal.getV)
    } else if (value.hasBool) {
      if (value.getBool.getV) 1 else 0
    } else {
      // fallback
      0
    }
  }

  private def fallbackToBytes(value: Value): Any = {
    if (value.hasBinary) value.getBinary.getV.toByteArray
    else fallbackToString(value).toString.getBytes
  }

  private def fallbackToString(value: Value): Any = {
    // If there's a string form, use it, else serialize the value to JSON
    if (value.hasString) value.getString.getV
    else dasValueToJsonString(value)
  }

  /**
   * Serializes a DAS Value into JSON (string). Useful when we have no direct typed mapping.
   */
  private def dasValueToJsonString(value: Value): String = {
    val node = buildJsonNode(value)
    mapper.writeValueAsString(node)
  }

  /**
   * Recursively builds a Jackson `JsonNode` from a DAS Value.
   */
  private def buildJsonNode(value: Value): JsonNode = {
    if (value.hasNull) {
      NullNode.instance
    } else if (value.hasBool) {
      nodeFactory.booleanNode(value.getBool.getV)
    } else if (value.hasInt) {
      nodeFactory.numberNode(value.getInt.getV)
    } else if (value.hasLong) {
      nodeFactory.numberNode(value.getLong.getV)
    } else if (value.hasShort) {
      nodeFactory.numberNode(value.getShort.getV)
    } else if (value.hasFloat) {
      nodeFactory.numberNode(value.getFloat.getV)
    } else if (value.hasDouble) {
      nodeFactory.numberNode(value.getDouble.getV)
    } else if (value.hasDecimal) {
      nodeFactory.numberNode(new java.math.BigDecimal(value.getDecimal.getV))
    } else if (value.hasString) {
      new TextNode(value.getString.getV)
    } else if (value.hasList) {
      val arr = nodeFactory.arrayNode()
      value.getList.getValuesList.forEach { v =>
        arr.add(buildJsonNode(v))
      }
      arr
    } else if (value.hasRecord) {
      val obj = nodeFactory.objectNode()
      value.getRecord.getAttsList.forEach { attr =>
        obj.set(attr.getName, buildJsonNode(attr.getValue))
      }
      obj
    } else {
      // fallback
      new TextNode(value.toString)
    }
  }

  /**
   * Converts an SQLite declared column type into a DAS Type. We try to handle all common SQLite types:
   *   - INTEGER (and synonyms, including TINYINT, MEDIUMINT, BIGINT)
   *   - REAL / FLOAT / DOUBLE
   *   - TEXT / CHAR / CLOB
   *   - BLOB
   *   - NUMERIC / DECIMAL
   *   - BOOLEAN
   *   - DATE / DATETIME / TIME / TIMESTAMP
   * and fallback if not recognized.
   */
  private def toDASType(sqliteType: String, nullable: Boolean): Either[String, Type] = {
    val builder = Type.newBuilder()
    val trimmedType = Option(sqliteType).map(_.trim.toLowerCase).getOrElse("")

    try {
      val dasType: Type =
        if (trimmedType.contains("int")) {
          // Covers "integer", "int", "tinyint", "smallint", "mediumint", "bigint", etc.
          builder.setLong(LongType.newBuilder().setNullable(nullable)).build()
        } else if (trimmedType.contains("real") || trimmedType.contains("floa") || trimmedType.contains("doub")) {
          // REAL, FLOAT, DOUBLE
          builder.setDouble(DoubleType.newBuilder().setNullable(nullable)).build()
        } else if (trimmedType.contains("blob")) {
          builder.setBinary(BinaryType.newBuilder().setNullable(nullable)).build()
        } else if (trimmedType.contains("text") || trimmedType.contains("char") || trimmedType.contains("clob")) {
          builder.setString(StringType.newBuilder().setNullable(nullable)).build()
        } else if (trimmedType.contains("numeric") || trimmedType.contains("dec")) {
          builder.setDecimal(DecimalType.newBuilder().setNullable(nullable)).build()
        } else if (trimmedType.contains("boolean")) {
          builder.setBool(BoolType.newBuilder().setNullable(nullable)).build()
        } else if (
          trimmedType.contains("date") ||
          trimmedType.contains("time") ||
          trimmedType.contains("timestamp") ||
          trimmedType.contains("datetime")
        ) {
          // This can be tricky, but we attempt to interpret as Timestamp if "datetime"/"timestamp",
          // as Date if "date" alone, as Time if "time" alone. If it's ambiguous, fallback to String.
          //
          // For the column definition, let's treat "date"/"datetime"/"timestamp" as Timestamp for the sake of
          // enabling more general usage. Or we can break it down further if needed.
          builder.setTimestamp(TimestampType.newBuilder().setNullable(nullable)).build()
        } else if (trimmedType.isEmpty || trimmedType == "null") {
          // No declared type
          builder.setString(StringType.newBuilder().setNullable(nullable)).build()
        } else {
          // fallback if we can't interpret
          throw new IllegalArgumentException(s"Unsupported or unknown type: $sqliteType")
        }

      Right(dasType)
    } catch {
      case e: IllegalArgumentException => Left(e.getMessage)
    }
  }

}
