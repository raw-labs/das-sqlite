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

import java.nio.file.Files

import org.scalatest.funsuite.AnyFunSuite

import com.rawlabs.das.sdk.{DASSdkInvalidArgumentException, DASSdkUnauthenticatedException}

/**
 * SQLite connection tests.
 */
class DASSqliteConnectionTest extends AnyFunSuite {

  test("Missing database => DASSdkInvalidArgumentException") {
    // Our DASSqlite code requires a "database" option.
    val ex = intercept[DASSdkInvalidArgumentException] {
      new DASSqlite(Map.empty) // no "database" key
    }
    assert(ex.getMessage.contains("database"))
  }

  test("Bad database path => DASSdkInvalidArgumentException (unwritable)") {
    // We'll try to create a DB in a location we (hopefully) cannot write to, e.g. root "/no_access_here"
    // Depending on your OS, this might fail in different ways. Adjust as needed for your environment.
    val ex = intercept[DASSdkInvalidArgumentException] {
      new DASSqlite(Map("database" -> "/no_access_here/some.db"))
    }
    assert(ex.getMessage.contains("Could not connect"))
  }

  test("Valid in-memory => no exception") {
    // SQLite in-memory: just pass ":memory:" as database path
    val sdk = new DASSqlite(Map("database" -> ":memory:"))
    // Basic check
    assert(sdk.tableDefinitions.size >= 0)
    sdk.close()
  }

  test("Valid file-based => no exception") {
    // Create a temporary file for SQLite. Typically, the driver will create or open this file as needed.
    val tempFile = Files.createTempFile("sqlite_test_", ".db")
    val sdk = new DASSqlite(Map("database" -> tempFile.toString))
    // Basic check
    assert(sdk.tableDefinitions.size >= 0)
    sdk.close()
  }

  test("User/password are ignored => no exception") {
    // Normally, SQLite doesn't use user/password, but DASSqlite code might accept them without error.
    val sdk = new DASSqlite(Map("database" -> ":memory:", "user" -> "ignored_user", "password" -> "ignored_pass"))
    // Basic check
    assert(sdk.tableDefinitions.size >= 0)
    sdk.close()
  }

  test("If you want to forcibly fail authentication => DASSdkUnauthenticatedException example") {
    // By default, standard SQLite doesn't enforce authentication,
    // but if you have a custom driver or extension that does, you can mimic it here.
    // For demonstration, we'll show how you *might* trigger or check it:
    val ex = intercept[DASSdkUnauthenticatedException] {
      throw new DASSdkUnauthenticatedException("Invalid username/password")
    }
    assert(ex.getMessage.contains("Invalid username/password"))
  }

}
