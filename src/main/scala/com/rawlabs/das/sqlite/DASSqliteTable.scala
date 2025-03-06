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

import scala.jdk.CollectionConverters._

import com.rawlabs.das.sdk.scala.DASTable
import com.rawlabs.das.sdk.scala.DASTable.TableEstimate
import com.rawlabs.das.sdk.{DASExecuteResult, DASSdkUnsupportedException}
import com.rawlabs.protocol.das.v1.query._
import com.rawlabs.protocol.das.v1.tables.{Row, TableDefinition}
import com.rawlabs.protocol.das.v1.types.Value
import com.rawlabs.protocol.das.v1.types.Value.ValueCase

/**
 * A DASTable implementation for SQLite.
 *
 * @param backend The DASSqliteBackend that provides actual DB access methods
 * @param defn The Protobuf-based TableDefinition describing column schema, etc.
 * @param maybePrimaryKey The optional single-column primary key discovered for this table
 */
class DASSqliteTable(backend: DASSqliteBackend, defn: TableDefinition, maybePrimaryKey: Option[String])
    extends DASTable {

  def definition: TableDefinition = defn

  /**
   * SQLite table might not have a single unique column. If it does not, we throw an exception. If it does, we return
   * that PK name.
   */
  override def uniqueColumn: String =
    maybePrimaryKey.getOrElse(throw new DASSdkUnsupportedException())

  /**
   * A naive table-size estimate. Builds a SQL query with the same WHERE conditions used in `execute(...)` and calls
   * backend.estimate(...) on it. SQLite's estimate is fairly simplistic.
   */
  override def tableEstimate(quals: Seq[Qual], columns: Seq[String]): TableEstimate = {
    // 1) Build the same WHERE clause used in `execute(...)`.
    val supportedQuals = quals.flatMap(qualToSql)

    val whereClause =
      if (supportedQuals.isEmpty) ""
      else "\nWHERE " + supportedQuals.mkString(" AND ")

    // 2) Possibly use columns if you want to estimate only the subset of columns,
    //    or just use "*" or "1" to get an overall row count approximation.
    val selectClause =
      if (columns.isEmpty) "1"
      else columns.map(quoteIdentifier).mkString(", ")

    val tableName = quoteIdentifier(defn.getTableId.getName)
    val sql =
      s"SELECT $selectClause FROM $tableName$whereClause"

    backend.estimate(sql)
  }

  /**
   * Returns the raw generated SQL as "explain" lines. In SQLite, we simply split the final query by newlines.
   */
  override def explain(
      quals: Seq[Qual],
      columns: Seq[String],
      sortKeys: Seq[SortKey],
      maybeLimit: Option[Long]): Seq[String] = {
    mkSQL(quals, columns, sortKeys, maybeLimit).split("\n").toSeq
  }

  /**
   * Executes a select-like query in SQLite, returning the raw results via a DASExecuteResult.
   */
  override def execute(
      quals: Seq[Qual],
      columns: Seq[String],
      sortKeys: Seq[SortKey],
      maybeLimit: Option[Long]): DASExecuteResult = {
    backend.execute(mkSQL(quals, columns, sortKeys, maybeLimit))
  }

  /**
   * Inserts a single row and returns the inserted row (including DB defaults, if any). In SQLite 3.35+, we can use
   * RETURNING * to fetch the inserted row(s).
   */
  override def insert(row: Row): Row = {
    bulkInsert(Seq(row)).head
  }

  /**
   * Inserts multiple rows, returning them all. If rows is empty, returns an empty sequence.
   */
  override def bulkInsert(rows: Seq[Row]): Seq[Row] = {
    if (rows.isEmpty) {
      return Seq.empty
    }

    // 1) Gather columns from the first row
    val firstRow = rows.head
    val columns = firstRow.getColumnsList.asScala.map(_.getName).toList
    // Typically, ensure all subsequent rows have the same columns in the same order

    // 2) Build placeholders for each row
    //    e.g., rowPlaceholder => "(?, ?, ...)" for the number of columns
    val numCols = columns.size
    val rowPlaceholder = "(" + List.fill(numCols)("?").mkString(", ") + ")"
    val placeholdersAll = List.fill(rows.size)(rowPlaceholder).mkString(", ")

    val columnNames = columns.map(quoteIdentifier).mkString(", ")

    val tableName = quoteIdentifier(defn.getTableId.getName)
    // 3.35+ syntax in SQLite for RETURNING:
    val insertSql =
      s"INSERT INTO $tableName ($columnNames) VALUES $placeholdersAll RETURNING *"

    // 3) Flatten all param values
    //    For each row, for each col in the same order, gather the Value
    val params: Seq[Value] = rows.flatMap { row =>
      row.getColumnsList.asScala.map(_.getData)
    }

    // 4) Execute and read the result set
    val result = backend.executeReturningRows(insertSql, params)

    val insertedRows = scala.collection.mutable.ListBuffer[Row]()
    try {
      while (result.hasNext) {
        insertedRows += result.next()
      }
    } finally {
      result.close()
    }
    insertedRows.toSeq
  }

  /**
   * Updates the row with the given rowId, returning the updated Row. If no row is matched, throws an exception.
   */
  override def update(rowId: Value, newValues: Row): Row = {
    // 1) Build the SET clause from columns in `newValues`
    val setClause = newValues.getColumnsList.asScala
      .map { col => s"${quoteIdentifier(col.getName)} = ?" }
      .mkString(", ")

    // 2) Build the SQL with RETURNING * (3.35+)
    val tableName = quoteIdentifier(defn.getTableId.getName)
    val updateSql =
      s"""
         |UPDATE $tableName
         |SET $setClause
         |WHERE ${quoteIdentifier(uniqueColumn)} = ?
         |RETURNING *
         |""".stripMargin

    // 3) Gather parameters: first all the newValues columns, then rowId last
    val params: Seq[Value] =
      newValues.getColumnsList.asScala.map(_.getData).toSeq :+ rowId

    // 4) Execute
    val result = backend.executeReturningRows(updateSql, params)
    try {
      if (!result.hasNext) {
        // Possibly means no row matched that id?
        result.close()
        throw new RuntimeException(s"Update failed: no row matched id=$rowId")
      }
      val updatedRow = result.next()

      // If you want to ensure only one row was updated, you could do:
      // if (result.hasNext) { ... log a warning or throw an error ... }

      updatedRow
    } finally {
      result.close()
    }
  }

  /**
   * Deletes the row with the given rowId.
   */
  override def delete(rowId: Value): Unit = {
    val tableName = quoteIdentifier(defn.getTableId.getName)
    val deleteSql =
      s"DELETE FROM $tableName WHERE ${quoteIdentifier(uniqueColumn)} = ?"

    // We can reuse the batchQuery interface to pass a single parameter set
    backend.batchQuery(deleteSql, Seq(Seq(rowId)))
  }

  // ------------------------------------------------------------------------------------------
  // SQL Construction
  // ------------------------------------------------------------------------------------------

  private def mkSQL(
      quals: Seq[Qual],
      columns: Seq[String],
      sortKeys: Seq[SortKey],
      maybeLimit: Option[Long]): String = {

    // Build SELECT list
    val selectClause =
      if (columns.isEmpty) "1"
      else columns.map(quoteIdentifier).mkString(", ")

    // Build WHERE from `quals`
    val supportedQuals = quals.flatMap(qualToSql)
    val whereClause =
      if (supportedQuals.isEmpty) ""
      else "\nWHERE " + supportedQuals.mkString(" AND ")

    // Build ORDER BY
    val orderByClause =
      if (sortKeys.isEmpty) ""
      else "\nORDER BY " + sortKeys.map(sortKeyToSql).mkString(", ")

    // Build LIMIT
    val limitClause = maybeLimit.map(l => s"\nLIMIT $l").getOrElse("")

    val tableName = quoteIdentifier(defn.getTableId.getName)
    s"SELECT $selectClause FROM $tableName$whereClause$orderByClause$limitClause"
  }

  /**
   * SQLite requires quotes around identifiers that might include unusual characters or match keywords. We double up
   * internal quotes, then wrap in double-quotes.
   */
  private def quoteIdentifier(ident: String): String = {
    val escaped = ident.replace("\"", "\"\"")
    s""""$escaped""""
  }

  /**
   * Convert a SortKey to an ORDER BY snippet. SQLite does not inherently support "NULLS FIRST/LAST", so you may ignore
   * or simulate it. We show a naive approach here.
   */
  private def sortKeyToSql(sk: SortKey): String = {
    val col = quoteIdentifier(sk.getName)
    val direction = if (sk.getIsReversed) "DESC" else "ASC"

    // SQLite doesn't have a built-in "NULLS FIRST/LAST" syntax, but we keep the code for demonstration
    val nullsPart =
      if (sk.getNullsFirst) " -- NULLS FIRST (not directly supported)" else " -- NULLS LAST (not directly supported)"

    // We can also handle collate if needed; SQLite supports "COLLATE nocase", etc.
    val collation = if (sk.getCollate.nonEmpty) s" COLLATE ${sk.getCollate}" else ""

    s"$col$collation $direction$nullsPart"
  }

  /**
   * Convert a single Value into an inline SQL literal. For dynamic queries used in e.g. EXPLAIN, or simplistic queries.
   * For actual data-binding, we typically use parameters, so be cautious about SQL injection for real usage.
   */
  private def valueToSql(v: Value): String = {
    v.getValueCase match {
      case ValueCase.NULL    => "NULL"
      case ValueCase.BYTE    => v.getByte.getV.toString
      case ValueCase.SHORT   => v.getShort.getV.toString
      case ValueCase.INT     => v.getInt.getV.toString
      case ValueCase.LONG    => v.getLong.getV.toString
      case ValueCase.FLOAT   => v.getFloat.getV.toString
      case ValueCase.DOUBLE  => v.getDouble.getV.toString
      case ValueCase.DECIMAL =>
        // Typically a numeric string, we just inline it
        v.getDecimal.getV
      case ValueCase.BOOL =>
        // In SQLite, "TRUE"/"FALSE" are not strictly special, but let's keep them as uppercase for clarity
        if (v.getBool.getV) "TRUE" else "FALSE"
      case ValueCase.STRING =>
        s"'${escape(v.getString.getV)}'"
      case ValueCase.BINARY =>
        val bytes = v.getBinary.getV.toByteArray
        // Convert to an SQLite-style blob literal: X'DEADBEEF'
        s"${byteArrayToSQLiteHex(bytes)}"

      case ValueCase.DATE =>
        // E.g., '2024-01-15'
        val d = v.getDate
        f"'${d.getYear}%04d-${d.getMonth}%02d-${d.getDay}%02d'"

      case ValueCase.TIME =>
        // E.g., '10:30:25'
        val t = v.getTime
        f"'${t.getHour}%02d:${t.getMinute}%02d:${t.getSecond}%02d'"

      case ValueCase.TIMESTAMP =>
        val ts = v.getTimestamp
        // E.g., '2024-01-15 10:30:25'
        f"'${ts.getYear}%04d-${ts.getMonth}%02d-${ts.getDay}%02d ${ts.getHour}%02d:${ts.getMinute}%02d:${ts.getSecond}%02d'"

      case ValueCase.INTERVAL | ValueCase.RECORD | ValueCase.LIST =>
        // For a simple example, store them as string or skip
        s"'${escape(v.toString)}'"

      case ValueCase.VALUE_NOT_SET =>
        "NULL"
    }
  }

  /**
   * SQLite-style hexadecimal blob literal: X'DEADBEEF'
   */
  private def byteArrayToSQLiteHex(byteArray: Array[Byte]): String = {
    val hexString = byteArray.map("%02x".format(_)).mkString
    s"X'$hexString'"
  }

  private def escape(str: String): String =
    str.replace("'", "''") // naive approach for single quotes

  /**
   * Maps an Operator enum to the corresponding SQL string. Some operators like ILIKE are not native to SQLite, so we do
   * not handle them.
   */
  private def operatorToSql(op: Operator): Option[String] = {
    op match {
      case Operator.EQUALS                => Some("=")
      case Operator.NOT_EQUALS            => Some("<>")
      case Operator.LESS_THAN             => Some("<")
      case Operator.LESS_THAN_OR_EQUAL    => Some("<=")
      case Operator.GREATER_THAN          => Some(">")
      case Operator.GREATER_THAN_OR_EQUAL => Some(">=")
      case Operator.LIKE                  => Some("LIKE")
      case Operator.NOT_LIKE              => Some("NOT LIKE")

      // May be less typical in a WHERE clause
      case Operator.PLUS  => Some("+")
      case Operator.MINUS => Some("-")
      case Operator.TIMES => Some("*")
      case Operator.DIV   => Some("/")
      case Operator.MOD   => Some("%")
      case Operator.AND   => Some("AND")
      case Operator.OR    => Some("OR")

      case _ => None
    }
  }

  /**
   * `IsAllQual` means "col op ALL these values", we interpret as multiple AND clauses
   */
  private def isAllQualToSql(colName: String, iq: IsAllQual): Option[String] = {
    operatorToSql(iq.getOperator) match {
      case Some(opStr) =>
        val clauses = iq.getValuesList.asScala.map(v => s"$colName $opStr ${valueToSql(v)}")
        // Combine with AND
        Some(clauses.mkString("(", " AND ", ")"))
      case None => None
    }
  }

  /**
   * `IsAnyQual` means "col op ANY of these values", we interpret as multiple OR clauses
   */
  private def isAnyQualToSql(colName: String, iq: IsAnyQual): Option[String] = {
    operatorToSql(iq.getOperator) match {
      case Some(opStr) =>
        val clauses = iq.getValuesList.asScala.map(v => s"$colName $opStr ${valueToSql(v)}")
        // Combine with OR
        Some(clauses.mkString("(", " OR ", ")"))
      case None => None
    }
  }

  /**
   * `SimpleQual` is a single condition: "col op value"
   */
  private def simpleQualToSql(colName: String, sq: SimpleQual): Option[String] = {
    if (sq.getValue.hasNull && sq.getOperator == Operator.EQUALS) {
      Some(s"$colName IS NULL")
    } else if (sq.getValue.hasNull && sq.getOperator == Operator.NOT_EQUALS) {
      Some(s"$colName IS NOT NULL")
    } else {
      operatorToSql(sq.getOperator) match {
        case Some(opStr) =>
          val valStr = valueToSql(sq.getValue)
          Some(s"$colName $opStr $valStr")
        case None => None
      }
    }
  }

  /**
   * Converts any `Qual` to a SQL snippet. We handle `SimpleQual`, `IsAnyQual`, or `IsAllQual`.
   */
  private def qualToSql(q: Qual): Option[String] = {
    val colName = quoteIdentifier(q.getName)
    if (q.hasSimpleQual) {
      simpleQualToSql(colName, q.getSimpleQual)
    } else
      q.getQualCase match {
        case Qual.QualCase.IS_ANY_QUAL => isAnyQualToSql(colName, q.getIsAnyQual)
        case Qual.QualCase.IS_ALL_QUAL => isAllQualToSql(colName, q.getIsAllQual)
        case _                         => throw new IllegalArgumentException(s"Unsupported qual: $q")
      }
  }

}
