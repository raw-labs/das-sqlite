/*
 * Copyright 2024 RAW Labs S.A.
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

import java.nio.file.{Files, Path}
import java.sql.DriverManager

import scala.jdk.CollectionConverters._
import scala.util.Using

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import com.google.protobuf.ByteString
import com.rawlabs.das.sdk.DASSdkInvalidArgumentException
import com.rawlabs.protocol.das.v1.query._
import com.rawlabs.protocol.das.v1.tables._
import com.rawlabs.protocol.das.v1.types._
import com.typesafe.scalalogging.StrictLogging

/**
 * Integration test suite covering the DASSqlite and DASSqliteTable classes, using a SQLite database.
 */
class DASSqliteTableTest extends AnyFunSuite with BeforeAndAfterAll with StrictLogging {

  private var tempFile: Path = _
  private var sdk: DASSqlite = _
  private var table: DASSqliteTable = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    // 1) Create a temporary file
    tempFile = Files.createTempFile("test_all_types_db", ".db")

    // 2) Connect manually (raw JDBC) and create the table
    val createTableDDL =
      """
        |CREATE TABLE IF NOT EXISTS all_types (
        |  id INTEGER PRIMARY KEY AUTOINCREMENT,
        |  col_integer     INTEGER,
        |  col_bigint      INTEGER,
        |  col_boolean     BOOLEAN,
        |  col_numeric     DECIMAL(20,2),
        |  col_real        REAL,
        |  col_double      DOUBLE,
        |  col_timestamp   TEXT,
        |  col_timestamptz TEXT,
        |  col_date        TEXT,
        |  col_time        TEXT,
        |  col_timetz      TEXT,
        |  col_text        TEXT,
        |  col_varchar     TEXT,
        |  col_bytea       BLOB,
        |  col_json        TEXT,
        |  col_jsonb       TEXT,
        |  col_hstore      TEXT,
        |
        |  col_int_array   TEXT,
        |  col_text_array  TEXT,
        |  col_date_array  TEXT,
        |  col_json_array  TEXT,
        |  col_jsonb_array TEXT,
        |  col_hstore_array TEXT
        |);
      """.stripMargin

    val jdbcUrl = s"jdbc:sqlite:${tempFile.toString}"
    try {
      Class.forName("org.sqlite.JDBC")
    } catch {
      case _: ClassNotFoundException =>
        throw new RuntimeException("SQLite JDBC driver not found on the classpath.")
    }
    Using.resource(DriverManager.getConnection(jdbcUrl)) { conn =>
      val st = conn.createStatement()
      st.execute(createTableDDL)
      st.close()
    }

    // 3) Now that the DB file is populated, instantiate DASSqlite
    //    We pass the path minus "jdbc:sqlite:" because DASSqlite adds that prefix internally
    val sdkOptions = Map("database" -> tempFile.toString)
    sdk = new DASSqlite(sdkOptions)

    // 4) Get the "all_types" table from the DASSqlite
    val maybeTable = sdk.getTable("all_types")
    assert(maybeTable.isDefined, "Expected 'all_types' to exist in the file-based DB.")
    table = maybeTable.get.asInstanceOf[DASSqliteTable]
  }

  override def afterAll(): Unit = {
    // 1) Close the SDK
    if (sdk != null) sdk.close()

    // 2) Remove the temp file
    if (tempFile != null) {
      Files.deleteIfExists(tempFile)
    }
    super.afterAll()
  }

  // ===========================================================================
  // Tests for DASSqlite (top-level) methods
  // ===========================================================================
  test("DASSqlite.tableDefinitions returns our test table") {
    val defs = sdk.tableDefinitions
    assert(defs.nonEmpty, "tableDefinitions should not be empty.")
    val names = defs.map(_.getTableId.getName)
    assert(names.contains("all_types"), "We expect 'all_types' table in the DB.")
  }

  test("DASSqlite.functionDefinitions is empty") {
    val funcs = sdk.functionDefinitions
    assert(funcs.isEmpty, "We do not define any functions, so functionDefinitions should be empty.")
  }

  test("DASSqlite.getTable returns a valid table, getFunction returns None") {
    val fakeTable = sdk.getTable("non_existent_table")
    assert(fakeTable.isEmpty, "getTable should return None for an invalid table name.")

    // getFunction always returns None in the current DASSqlite implementation
    val func = sdk.getFunction("someFunctionName")
    assert(func.isEmpty)
  }

  // ===========================================================================
  // Tests for DASSqliteTable public methods
  // ===========================================================================

  test("DASSqliteTable.definition is consistent with our table schema") {
    val defn = table.definition
    assert(defn.getTableId.getName.equalsIgnoreCase("all_types"), "Expected table name 'all_types'")
    assert(defn.getColumnsCount > 0, "We expect at least 1 column in the schema.")

    val columnNames = defn.getColumnsList.asScala.map(_.getName).toSet
    val expectedSomeColumns = Set(
      "id",
      "col_integer",
      "col_bigint",
      "col_boolean",
      "col_numeric",
      "col_real",
      "col_double",
      "col_timestamp",
      "col_timestamptz",
      "col_bytea",
      "col_json",
      "col_jsonb",
      "col_hstore")
    assert(expectedSomeColumns.subsetOf(columnNames), s"Expected columns $expectedSomeColumns to be present.")
  }

  test("DASSqliteTable.uniqueColumn returns 'id' if single primary key") {
    // If we discovered a single PK named "id", it should be returned. Otherwise, we might
    // not have a single PK. (By default, 'id INTEGER PRIMARY KEY AUTOINCREMENT' is the PK.)
    val uniqueCol = table.uniqueColumn
    assert(uniqueCol.equalsIgnoreCase("id"), "We expect 'id' to be the single PK for all_types.")
  }

  test("tableEstimate - naive in SQLite") {
    // Insert one row
    val inserted = insertSingleRowWithInt(999999)
    val idVal = getRowId(inserted)

    // Create a Qual that checks col_integer = 999999
    val qual = Qual
      .newBuilder()
      .setName("col_integer")
      .setSimpleQual(
        SimpleQual
          .newBuilder()
          .setOperator(Operator.EQUALS)
          .setValue(Value.newBuilder().setInt(ValueInt.newBuilder().setV(999999))))
      .build()

    val estimate = table.tableEstimate(Seq(qual), Seq("col_integer"))
    logger.info(s"Estimate => rowCount=${estimate.expectedNumberOfRows}, avgRowWidth=${estimate.avgRowWidthBytes}")
    // SQLite doesn't produce real estimates by default, so we may just see a naive (100,100).
    assert(estimate.expectedNumberOfRows > 0)
    table.delete(idVal)
  }

  test("explain - we simply split the final SQL in lines") {
    val qual = Qual
      .newBuilder()
      .setName("col_integer")
      .setSimpleQual(
        SimpleQual
          .newBuilder()
          .setOperator(Operator.LESS_THAN)
          .setValue(Value.newBuilder().setInt(ValueInt.newBuilder().setV(1000000))))
      .build()

    val sortKey = SortKey
      .newBuilder()
      .setName("col_integer")
      .setIsReversed(true)
      .build()

    val explainLines = table.explain(
      quals = Seq(qual),
      columns = Seq("id", "col_integer"),
      sortKeys = Seq(sortKey),
      maybeLimit = Some(10))

    logger.info("Explain lines for SQLite:")
    explainLines.foreach(ln => logger.info(ln))

    assert(explainLines.nonEmpty)
    assert(explainLines.head.startsWith("SELECT "), "Expected a SELECT statement.")
  }

  test("execute with projections") {
    // Insert a row with col_varchar
    val rowToInsert = Row
      .newBuilder()
      .addColumns(
        Column
          .newBuilder()
          .setName("col_varchar")
          .setData(Value.newBuilder().setString(ValueString.newBuilder().setV("ProjectionTestRow"))))
      .build()
    table.insert(rowToInsert)

    // Qual to filter on col_varchar
    val qual = Qual
      .newBuilder()
      .setName("col_varchar")
      .setSimpleQual(
        SimpleQual
          .newBuilder()
          .setOperator(Operator.EQUALS)
          .setValue(Value.newBuilder().setString(ValueString.newBuilder().setV("ProjectionTestRow"))))
      .build()

    // Ask only for 'id' and 'col_varchar'
    val result = table.execute(Seq(qual), Seq("id", "col_varchar"), Seq.empty, None)
    assert(result.hasNext, "Expected to find the inserted row.")
    val found = result.next()
    result.close()
    val colMap = found.getColumnsList.asScala.map(c => c.getName -> c.getData).toMap
    assert(colMap.size == 2)
    assert(colMap.contains("id"))
    assert(colMap.contains("col_varchar"))

    // Cleanup
    table.delete(colMap("id"))
  }

  test("execute with filtering") {
    val rowA = insertSingleRowWithVarchar("FilteringTestA")
    val rowB = insertSingleRowWithVarchar("FilteringTestB")

    // Qual B
    val qualB = Qual
      .newBuilder()
      .setName("col_varchar")
      .setSimpleQual(
        SimpleQual
          .newBuilder()
          .setOperator(Operator.EQUALS)
          .setValue(Value.newBuilder().setString(ValueString.newBuilder().setV("FilteringTestB"))))
      .build()

    val resultB = table.execute(Seq(qualB), Seq("id", "col_varchar"), Seq.empty, None)
    assert(resultB.hasNext)
    val foundB = resultB.next()
    resultB.close()
    val mapB = foundB.getColumnsList.asScala.map(c => c.getName -> c.getData).toMap
    assert(mapB("col_varchar").hasString && mapB("col_varchar").getString.getV == "FilteringTestB")

    // No match
    val qualZ = Qual
      .newBuilder()
      .setName("col_varchar")
      .setSimpleQual(
        SimpleQual
          .newBuilder()
          .setOperator(Operator.EQUALS)
          .setValue(Value.newBuilder().setString(ValueString.newBuilder().setV("FilteringTestZ"))))
      .build()
    val resultZ = table.execute(Seq(qualZ), Seq("id"), Seq.empty, None)
    assert(!resultZ.hasNext)
    resultZ.close()

    table.delete(getRowId(rowA))
    table.delete(getRowId(rowB))
  }

  test("execute with sorting") {
    val row10 = insertSingleRowWithInt(10)
    val row20 = insertSingleRowWithInt(20)
    val row30 = insertSingleRowWithInt(30)

    // Desc sort
    val sortKey = SortKey
      .newBuilder()
      .setName("col_integer")
      .setIsReversed(true)
      .build()

    val result = table.execute(Seq.empty, Seq("col_integer"), Seq(sortKey), None)
    val returnedInts = consumeAllRows(result).flatMap { r =>
      r.getColumnsList.asScala.find(_.getName == "col_integer").map(_.getData.getInt.getV)
    }
    val ours = returnedInts.filter(Set(10, 20, 30))
    assert(ours == List(30, 20, 10), s"Expected [30, 20, 10], got $ours")

    table.delete(getRowId(row10))
    table.delete(getRowId(row20))
    table.delete(getRowId(row30))
  }

  test("execute with limiting") {
    val inserted = (1 to 5).map { i => insertSingleRowWithInt(1000 + i) }
    val result = table.execute(Seq.empty, Seq("id", "col_integer"), Seq.empty, Some(2))
    val rowsFetched = consumeAllRows(result)
    assert(rowsFetched.size == 2)
    inserted.foreach(r => table.delete(getRowId(r)))
  }

  test("insert a row with multiple columns and verify (some columns not natively typed in SQLite)") {
    val row = buildAllTypesRow()
    table.insert(row)

    // Now select back by col_integer=555
    val qual555 = Qual
      .newBuilder()
      .setName("col_integer")
      .setSimpleQual(
        SimpleQual
          .newBuilder()
          .setOperator(Operator.EQUALS)
          .setValue(Value.newBuilder().setInt(ValueInt.newBuilder().setV(555))))
      .build()

    val columnsToFetch = Seq(
      "id",
      "col_integer",
      "col_bigint",
      "col_numeric",
      "col_real",
      "col_double",
      "col_date",
      "col_timestamp",
      "col_timestamptz",
      "col_time",
      "col_timetz",
      "col_text",
      "col_varchar",
      "col_boolean",
      "col_bytea",
      "col_json",
      "col_jsonb",
      "col_hstore",
      "col_int_array",
      "col_text_array",
      "col_date_array",
      "col_json_array",
      "col_jsonb_array",
      "col_hstore_array")

    val selectResult = table.execute(Seq(qual555), columnsToFetch, Seq.empty, None)
    assert(selectResult.hasNext)
    val rowRead = selectResult.next()
    selectResult.close()
    val readMap = rowRead.getColumnsList.asScala.map(col => col.getName -> col.getData).toMap

    // Spot-check a few important columns
    assert(readMap("col_integer").hasInt && readMap("col_integer").getInt.getV == 555)
    assert(readMap("col_bigint").hasLong && readMap("col_bigint").getLong.getV == 9876543210123L)
    assert(readMap("col_numeric").hasDecimal && readMap("col_numeric").getDecimal.getV == "54321.01")
    assert(readMap("col_boolean").hasBool && readMap("col_boolean").getBool.getV)

    // Clean up
    table.delete(readMap("id"))
  }

  test("update works") {
    val rowToInsert = Row
      .newBuilder()
      .addColumns(
        Column
          .newBuilder()
          .setName("col_varchar")
          .setData(Value.newBuilder().setString(ValueString.newBuilder().setV("UpdateTestRow"))))
      .addColumns(
        Column
          .newBuilder()
          .setName("col_text")
          .setData(Value.newBuilder().setString(ValueString.newBuilder().setV("Original text"))))
      .build()

    val inserted = table.insert(rowToInsert)
    val insertedMap = inserted.getColumnsList.asScala.map(c => c.getName -> c.getData).toMap
    val rowId = insertedMap("id")

    // Update col_text
    val newTextRow = Row
      .newBuilder()
      .addColumns(
        Column
          .newBuilder()
          .setName("col_text")
          .setData(Value.newBuilder().setString(ValueString.newBuilder().setV("Updated text"))))
      .build()
    table.update(rowId, newTextRow)

    // Check
    val qualById = Qual
      .newBuilder()
      .setName("id")
      .setSimpleQual(SimpleQual.newBuilder().setOperator(Operator.EQUALS).setValue(rowId))
      .build()
    val updatedRes = table.execute(Seq(qualById), Seq("col_text"), Seq.empty, None)
    assert(updatedRes.hasNext)
    val updated = updatedRes.next()
    updatedRes.close()
    val updatedMap = updated.getColumnsList.asScala.map(c => c.getName -> c.getData).toMap
    assert(updatedMap("col_text").hasString && updatedMap("col_text").getString.getV == "Updated text")

    // cleanup
    table.delete(rowId)
  }

  test("delete works") {
    val rowToInsert = Row
      .newBuilder()
      .addColumns(
        Column
          .newBuilder()
          .setName("col_varchar")
          .setData(Value.newBuilder().setString(ValueString.newBuilder().setV("DeleteTestRow"))))
      .build()
    val inserted = table.insert(rowToInsert)
    val insertedMap = inserted.getColumnsList.asScala.map(c => c.getName -> c.getData).toMap
    val rowId = insertedMap("id")

    // Confirm presence
    val q = Qual
      .newBuilder()
      .setName("col_varchar")
      .setSimpleQual(
        SimpleQual
          .newBuilder()
          .setOperator(Operator.EQUALS)
          .setValue(Value.newBuilder().setString(ValueString.newBuilder().setV("DeleteTestRow"))))
      .build()
    val resBefore = table.execute(Seq(q), Seq("id"), Seq.empty, None)
    assert(resBefore.hasNext)
    resBefore.close()

    table.delete(rowId)

    val resAfter = table.execute(Seq(q), Seq("id"), Seq.empty, None)
    assert(!resAfter.hasNext)
    resAfter.close()
  }

  test("bulkInsert works") {
    val rows = (1 to 3).map { i =>
      Row
        .newBuilder()
        .addColumns(
          Column
            .newBuilder()
            .setName("col_varchar")
            .setData(Value.newBuilder().setString(ValueString.newBuilder().setV(s"BulkTestRow_$i"))))
        .build()
    }

    table.bulkInsert(rows)

    // Verify
    val qual = Qual
      .newBuilder()
      .setName("col_varchar")
      .setIsAnyQual(
        IsAnyQual
          .newBuilder()
          .setOperator(Operator.LIKE)
          .addValues(Value.newBuilder().setString(ValueString.newBuilder().setV("BulkTestRow_%"))))
      .build()

    val result = table.execute(Seq(qual), Seq("id", "col_varchar"), Seq.empty, None)
    val foundRows = consumeAllRows(result)
    val foundTexts = foundRows.flatMap { r =>
      r.getColumnsList.asScala.find(_.getName == "col_varchar").map(_.getData.getString.getV)
    }
    val expected = (1 to 3).map(i => s"BulkTestRow_$i").toSet
    val intersects = foundTexts.toSet intersect expected
    assert(intersects.size >= 3, s"Should find all 3 inserted rows: $foundTexts")

    // Cleanup
    foundRows.foreach { row =>
      val map = row.getColumnsList.asScala.map(c => c.getName -> c.getData).toMap
      if (map.get("col_varchar").exists(_.getString.getV.startsWith("BulkTestRow_"))) {
        table.delete(map("id"))
      }
    }
  }

  // ===========================================================================
  // Helper Functions
  // ===========================================================================

  /** Insert a row with col_integer = `value`. Return the row returned by table.insert. */
  private def insertSingleRowWithInt(value: Int): Row = {
    val row = Row
      .newBuilder()
      .addColumns(
        Column
          .newBuilder()
          .setName("col_integer")
          .setData(Value.newBuilder().setInt(ValueInt.newBuilder().setV(value))))
      .build()
    table.insert(row)
  }

  /** Insert a row with col_varchar = `value`. Return the row from table.insert. */
  private def insertSingleRowWithVarchar(value: String): Row = {
    val row = Row
      .newBuilder()
      .addColumns(
        Column
          .newBuilder()
          .setName("col_varchar")
          .setData(Value.newBuilder().setString(ValueString.newBuilder().setV(value))))
      .build()
    table.insert(row)
  }

  /** Retrieve the 'id' column from an inserted Row (we treat it as PK). */
  private def getRowId(inserted: Row): Value = {
    val colOpt = inserted.getColumnsList.asScala.find(_.getName.equalsIgnoreCase("id"))
    colOpt.getOrElse {
      throw new DASSdkInvalidArgumentException("No 'id' column found in inserted row.")
    }.getData
  }

  /** Utility to consume all rows from a DASExecuteResult. */
  private def consumeAllRows(execResult: com.rawlabs.das.sdk.DASExecuteResult): List[Row] = {
    val buf = scala.collection.mutable.ListBuffer[Row]()
    while (execResult.hasNext) {
      buf += execResult.next()
    }
    execResult.close()
    buf.toList
  }

  /**
   * Build a row with all the columns from your snippet set with sample values. Some columns (like hstore, JSONB, etc.)
   * will be stored as TEXT in SQLite.
   */
  private def buildAllTypesRow(): Row = {
    Row
      .newBuilder()
      .addColumns(Column
        .newBuilder()
        .setName("col_integer")
        .setData(Value.newBuilder().setInt(ValueInt.newBuilder().setV(555))))
      .addColumns(Column
        .newBuilder()
        .setName("col_bigint")
        .setData(Value.newBuilder().setLong(ValueLong.newBuilder().setV(9876543210123L))))
      .addColumns(Column
        .newBuilder()
        .setName("col_numeric")
        .setData(Value.newBuilder().setDecimal(ValueDecimal.newBuilder().setV("54321.01"))))
      .addColumns(Column
        .newBuilder()
        .setName("col_real")
        .setData(Value.newBuilder().setFloat(ValueFloat.newBuilder().setV(1.234f))))
      .addColumns(Column
        .newBuilder()
        .setName("col_double")
        .setData(Value.newBuilder().setDouble(ValueDouble.newBuilder().setV(9.876))))
      .addColumns(Column
        .newBuilder()
        .setName("col_date")
        .setData(Value.newBuilder().setDate(ValueDate.newBuilder().setYear(2025).setMonth(1).setDay(1))))
      .addColumns(Column
        .newBuilder()
        .setName("col_timestamp")
        .setData(Value
          .newBuilder()
          .setTimestamp(
            ValueTimestamp.newBuilder().setYear(2025).setMonth(1).setDay(1).setHour(23).setMinute(59).setSecond(59))))
      .addColumns(Column
        .newBuilder()
        .setName("col_timestamptz")
        .setData(Value
          .newBuilder()
          .setTimestamp(
            ValueTimestamp.newBuilder().setYear(2025).setMonth(1).setDay(1).setHour(23).setMinute(59).setSecond(59))))
      .addColumns(Column
        .newBuilder()
        .setName("col_time")
        .setData(Value.newBuilder().setTime(ValueTime.newBuilder().setHour(23).setMinute(59).setSecond(59))))
      .addColumns(Column
        .newBuilder()
        .setName("col_timetz")
        .setData(Value.newBuilder().setTime(ValueTime.newBuilder().setHour(23).setMinute(59).setSecond(59))))
      .addColumns(Column
        .newBuilder()
        .setName("col_text")
        .setData(Value.newBuilder().setString(ValueString.newBuilder().setV("Test all types"))))
      .addColumns(Column
        .newBuilder()
        .setName("col_varchar")
        .setData(Value.newBuilder().setString(ValueString.newBuilder().setV("Test varchar"))))
      .addColumns(Column
        .newBuilder()
        .setName("col_boolean")
        .setData(Value.newBuilder().setBool(ValueBool.newBuilder().setV(true))))
      .addColumns(Column
        .newBuilder()
        .setName("col_bytea")
        .setData(Value.newBuilder().setBinary(ValueBinary.newBuilder().setV(ByteString.fromHex("A1B2C3D4")))))
      .addColumns(
        Column
          .newBuilder()
          .setName("col_json")
          .setData(
            Value
              .newBuilder()
              .setRecord(
                ValueRecord
                  .newBuilder()
                  .addAtts(ValueRecordAttr
                    .newBuilder()
                    .setName("foo")
                    .setValue(Value.newBuilder().setString(ValueString.newBuilder().setV("bar")))))))
      .addColumns(
        Column
          .newBuilder()
          .setName("col_jsonb")
          .setData(
            Value
              .newBuilder()
              .setRecord(
                ValueRecord
                  .newBuilder()
                  .addAtts(ValueRecordAttr
                    .newBuilder()
                    .setName("foo")
                    .setValue(Value.newBuilder().setString(ValueString.newBuilder().setV("bar")))))))
      .addColumns(
        Column
          .newBuilder()
          .setName("col_hstore")
          .setData(
            Value
              .newBuilder()
              .setRecord(
                ValueRecord
                  .newBuilder()
                  .addAtts(ValueRecordAttr
                    .newBuilder()
                    .setName("a")
                    .setValue(Value.newBuilder().setString(ValueString.newBuilder().setV("1"))))
                  .addAtts(ValueRecordAttr
                    .newBuilder()
                    .setName("b")
                    .setValue(Value.newBuilder().setString(ValueString.newBuilder().setV("2")))))))
      .addColumns(
        Column
          .newBuilder()
          .setName("col_int_array")
          .setData(
            Value
              .newBuilder()
              .setList(ValueList
                .newBuilder()
                .addAllValues(Seq(
                  Value.newBuilder().setInt(ValueInt.newBuilder().setV(1)).build(),
                  Value.newBuilder().setInt(ValueInt.newBuilder().setV(2)).build(),
                  Value.newBuilder().setInt(ValueInt.newBuilder().setV(3)).build()).asJava))))
      .addColumns(
        Column
          .newBuilder()
          .setName("col_text_array")
          .setData(
            Value
              .newBuilder()
              .setList(ValueList
                .newBuilder()
                .addAllValues(Seq(
                  Value.newBuilder().setString(ValueString.newBuilder().setV("alpha")).build(),
                  Value.newBuilder().setString(ValueString.newBuilder().setV("beta")).build()).asJava))))
      .addColumns(
        Column
          .newBuilder()
          .setName("col_date_array")
          .setData(
            Value
              .newBuilder()
              .setList(ValueList
                .newBuilder()
                .addAllValues(Seq(
                  Value.newBuilder().setDate(ValueDate.newBuilder().setYear(2025).setMonth(1).setDay(1)).build(),
                  Value
                    .newBuilder()
                    .setDate(ValueDate.newBuilder().setYear(2025).setMonth(1).setDay(2))
                    .build()).asJava))))
      .addColumns(
        Column
          .newBuilder()
          .setName("col_json_array")
          .setData(
            Value
              .newBuilder()
              .setList(ValueList
                .newBuilder()
                .addAllValues(Seq(
                  Value
                    .newBuilder()
                    .setRecord(ValueRecord
                      .newBuilder()
                      .addAtts(ValueRecordAttr
                        .newBuilder()
                        .setName("k1")
                        .setValue(Value.newBuilder().setString(ValueString.newBuilder().setV("v1")))))
                    .build(),
                  Value
                    .newBuilder()
                    .setRecord(ValueRecord
                      .newBuilder()
                      .addAtts(ValueRecordAttr
                        .newBuilder()
                        .setName("k2")
                        .setValue(Value.newBuilder().setString(ValueString.newBuilder().setV("v2")))))
                    .build()).asJava))))
      .addColumns(
        Column
          .newBuilder()
          .setName("col_jsonb_array")
          .setData(
            Value
              .newBuilder()
              .setList(ValueList
                .newBuilder()
                .addAllValues(Seq(
                  Value
                    .newBuilder()
                    .setRecord(ValueRecord
                      .newBuilder()
                      .addAtts(ValueRecordAttr
                        .newBuilder()
                        .setName("k1")
                        .setValue(Value.newBuilder().setString(ValueString.newBuilder().setV("v1")))))
                    .build(),
                  Value
                    .newBuilder()
                    .setRecord(ValueRecord
                      .newBuilder()
                      .addAtts(ValueRecordAttr
                        .newBuilder()
                        .setName("k2")
                        .setValue(Value.newBuilder().setString(ValueString.newBuilder().setV("v2")))))
                    .build()).asJava))))
      .addColumns(
        Column
          .newBuilder()
          .setName("col_hstore_array")
          .setData(
            Value
              .newBuilder()
              .setList(ValueList
                .newBuilder()
                .addAllValues(Seq(
                  Value
                    .newBuilder()
                    .setRecord(
                      ValueRecord
                        .newBuilder()
                        .addAtts(ValueRecordAttr
                          .newBuilder()
                          .setName("x")
                          .setValue(Value.newBuilder().setString(ValueString.newBuilder().setV("10"))))
                        .addAtts(ValueRecordAttr
                          .newBuilder()
                          .setName("y")
                          .setValue(Value.newBuilder().setString(ValueString.newBuilder().setV("20")))))
                    .build(),
                  Value
                    .newBuilder()
                    .setRecord(
                      ValueRecord
                        .newBuilder()
                        .addAtts(ValueRecordAttr
                          .newBuilder()
                          .setName("a")
                          .setValue(Value.newBuilder().setString(ValueString.newBuilder().setV("1"))))
                        .addAtts(ValueRecordAttr
                          .newBuilder()
                          .setName("b")
                          .setValue(Value.newBuilder().setString(ValueString.newBuilder().setV("2")))))
                    .build()).asJava))))
      .build()
  }

}
