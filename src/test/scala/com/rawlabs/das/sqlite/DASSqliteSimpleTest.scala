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

import scala.jdk.CollectionConverters._

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import com.rawlabs.protocol.das.v1.tables.{Column, Row}
import com.rawlabs.protocol.das.v1.types.{Value, ValueDouble, ValueInt, ValueString}
import com.typesafe.scalalogging.StrictLogging

class DASSqliteSimpleTest extends AnyFunSuite with BeforeAndAfterAll with StrictLogging {

  // ===========================================================================
  // Tests for DASSqlite (top-level) methods
  // ===========================================================================
  test("read mydb file") {
    val resourceUrl = getClass.getResource("/mydb")
    val file = new java.io.File(resourceUrl.toURI)
    val fullPath = file.getAbsolutePath

    val sdk = new DASSqlite(Map("database" -> fullPath))
    val defs = sdk.tableDefinitions
    assert(defs.nonEmpty, "tableDefinitions should not be empty.")
    val names = defs.map(_.getTableId.getName)
    assert(names.contains("COMPANY"), "We expect 'COMPANY' table in the DB.")

    val columns = defs.find(_.getTableId.getName == "COMPANY").get.getColumnsList.asScala
    assert(columns.map(_.getName) == Seq("ID", "NAME", "AGE", "ADDRESS", "SALARY"), "Columns should match.")

    val rs =
      sdk.getTable("COMPANY").get.execute(Seq.empty, Seq("ID", "NAME", "AGE", "ADDRESS", "SALARY"), Seq.empty, None)
    val buf = scala.collection.mutable.ListBuffer[Row]()
    while (rs.hasNext) {
      buf += rs.next()
    }
    rs.close()

    assert(
      buf.toList == List(
        buildMyDbRow(1, "Paul", 32, "California", 20000.0),
        buildMyDbRow(2, "Allen", 25, "Texas", 15000.0),
        buildMyDbRow(3, "Teddy", 23, "Norway", 20000.0),
        buildMyDbRow(4, "Mark", 25, "Rich-Mond ", 65000.0),
        buildMyDbRow(5, "David", 27, "Texas", 85000.0),
        buildMyDbRow(6, "Kim", 22, "South-Hall", 45000.0)))

    sdk.close()
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
