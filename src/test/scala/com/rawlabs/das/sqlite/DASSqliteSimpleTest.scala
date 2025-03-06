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

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import com.rawlabs.das.sdk.DASExecuteResult
import com.rawlabs.protocol.das.v1.query.{Operator, Qual, SimpleQual}
import com.rawlabs.protocol.das.v1.tables.{Column, Row}
import com.rawlabs.protocol.das.v1.types.{Value, ValueDouble, ValueInt, ValueString}
import com.typesafe.scalalogging.StrictLogging

class DASSqliteSimpleTest extends AnyFunSuite with BeforeAndAfterAll with StrictLogging {

  private var sdk: DASSqlite = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    sdk = buildSdk()
  }

  override def afterAll(): Unit = {
    sdk.close()
    super.afterAll()
  }

  test("read mydb file") {
    val defs = sdk.tableDefinitions
    assert(defs.nonEmpty, "tableDefinitions should not be empty.")
    val names = defs.map(_.getTableId.getName)
    assert(names.contains("COMPANY"), "We expect 'COMPANY' table in the DB.")

    val columns = defs.find(_.getTableId.getName == "COMPANY").get.getColumnsList.asScala
    assert(columns.map(_.getName) == Seq("ID", "NAME", "AGE", "ADDRESS", "SALARY"), "Columns should match.")

    val rs =
      sdk.getTable("COMPANY").get.execute(Seq.empty, Seq("ID", "NAME", "AGE", "ADDRESS", "SALARY"), Seq.empty, None)
    val buf = collectAllRows(rs)

    assert(
      buf.toList == List(
        buildMyDbRow(1, "Paul", 32, "California", 20000.0),
        buildMyDbRow(2, "Allen", 25, "Texas", 15000.0),
        buildMyDbRow(3, "Teddy", 23, "Norway", 20000.0),
        buildMyDbRow(4, "Mark", 25, "Rich-Mond ", 65000.0),
        buildMyDbRow(5, "David", 27, "Texas", 85000.0),
        buildMyDbRow(6, "Kim", 22, "South-Hall", 45000.0)))
  }

  test("filter mydb with operation that pushes down") {
    val rs =
      sdk
        .getTable("COMPANY")
        .get
        .execute(
          Seq(
            Qual
              .newBuilder()
              .setName("ID")
              .setSimpleQual(
                SimpleQual
                  .newBuilder()
                  .setOperator(Operator.EQUALS)
                  .setValue(Value.newBuilder().setInt(ValueInt.newBuilder().setV(1)))
                  .build())
              .build()),
          Seq("ID", "NAME", "AGE", "ADDRESS", "SALARY"),
          Seq.empty,
          None)
    val buf = collectAllRows(rs)
    assert(buf.toList == List(buildMyDbRow(1, "Paul", 32, "California", 20000.0)))
  }

  test("filter mydb with operation that does NOT push down") {
    val rs =
      sdk
        .getTable("COMPANY")
        .get
        .execute(
          Seq(
            Qual
              .newBuilder()
              .setName("NAME")
              .setSimpleQual(
                SimpleQual
                  .newBuilder()
                  .setOperator(Operator.ILIKE)
                  .setValue(Value.newBuilder().setString(ValueString.newBuilder().setV("PAUL")))
                  .build())
              .build()),
          Seq("ID", "NAME", "AGE", "ADDRESS", "SALARY"),
          Seq.empty,
          None)
    val buf = collectAllRows(rs)

    // Since we do NOT push down, we return the entire table
    assert(
      buf.toList == List(
        buildMyDbRow(1, "Paul", 32, "California", 20000.0),
        buildMyDbRow(2, "Allen", 25, "Texas", 15000.0),
        buildMyDbRow(3, "Teddy", 23, "Norway", 20000.0),
        buildMyDbRow(4, "Mark", 25, "Rich-Mond ", 65000.0),
        buildMyDbRow(5, "David", 27, "Texas", 85000.0),
        buildMyDbRow(6, "Kim", 22, "South-Hall", 45000.0)))
  }

  private def buildSdk(): DASSqlite = {
    val resourceUrl = getClass.getResource("/mydb")
    val file = new java.io.File(resourceUrl.toURI)
    val fullPath = file.getAbsolutePath

    new DASSqlite(Map("database" -> fullPath))
  }

  private def collectAllRows(rs: DASExecuteResult): Seq[Row] = {
    val buf = scala.collection.mutable.ListBuffer[Row]()
    while (rs.hasNext) {
      buf += rs.next()
    }
    rs.close()
    buf.toList
  }

  private def buildMyDbRow(id: Int, name: String, age: Int, address: String, salary: Double): Row = {
    val row = Row.newBuilder()
    row.addColumns(Column.newBuilder().setName("ID").setData(Value.newBuilder().setInt(ValueInt.newBuilder().setV(id))))
    row.addColumns(
      Column.newBuilder().setName("NAME").setData(Value.newBuilder().setString(ValueString.newBuilder().setV(name))))
    row.addColumns(
      Column.newBuilder().setName("AGE").setData(Value.newBuilder().setInt(ValueInt.newBuilder().setV(age))))
    row.addColumns(
      Column
        .newBuilder()
        .setName("ADDRESS")
        .setData(Value.newBuilder().setString(ValueString.newBuilder().setV(address))))
    row.addColumns(
      Column
        .newBuilder()
        .setName("SALARY")
        .setData(Value.newBuilder().setDouble(ValueDouble.newBuilder().setV(salary))))
    row.build()
  }

}
