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

import java.sql.SQLException

import com.rawlabs.das.sdk.DASSdkInvalidArgumentException
import com.rawlabs.das.sdk.scala.{DASFunction, DASSdk, DASTable}
import com.rawlabs.protocol.das.v1.functions.FunctionDefinition
import com.rawlabs.protocol.das.v1.tables.TableDefinition
import com.zaxxer.hikari.pool.HikariPool.PoolInitializationException
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}

/**
 * A DASSdk implementation for SQLite.
 *
 * Usage:
 *   - Provide a "database" option to specify the SQLite file path (or ":memory:" for in-memory).
 *
 * @param options Configuration map for the SDK, e.g.: "database" -> "/path/to/my.db"
 */
class DASSqlite(options: Map[String, String]) extends DASSdk {

  // ------------------------------------------------------------------------------------------------------
  // Parsing of options
  // ------------------------------------------------------------------------------------------------------

  /**
   * SQLite connections typically only need a file path or special tokens like ":memory:".
   */
  private val databasePath =
    options.getOrElse("database", throw new DASSdkInvalidArgumentException("database is required"))

  // Construct a JDBC URL for SQLite. Ex: "jdbc:sqlite:/path/to/dbFile" or "jdbc:sqlite::memory:".
  private val url = s"jdbc:sqlite:$databasePath"

  // ------------------------------------------------------------------------------------------------------
  // HikariCP Connection Pool Setup
  // ------------------------------------------------------------------------------------------------------
  try {
    // If using the default SQLite JDBC driver, ensure it's available
    Class.forName("org.sqlite.JDBC")
  } catch {
    case _: ClassNotFoundException =>
      throw new DASSdkInvalidArgumentException("SQLite JDBC driver not found on the classpath.")
  }

  private val hikariConfig = new HikariConfig()
  hikariConfig.setJdbcUrl(url)

  // Tune Hikari as needed
  hikariConfig.setMaximumPoolSize(10)
  hikariConfig.setConnectionTimeout(3000)

  // 1) Create HikariDataSource with error handling
  protected[sqlite] val dataSource = createDataSourceOrThrow(hikariConfig, url)

  // 2) Create backend, then attempt to retrieve tables
  private val backend = new DASSqliteBackend(dataSource)
  private val tables =
    try {
      backend.tables() // Immediately queries DB to list tables
    } catch {
      case sqlEx: SQLException =>
        // For SQLite, there's not a standard SQLState for "invalid credentials", but let's handle gracefully anyway
        throw new DASSdkInvalidArgumentException(s"Could not connect: ${sqlEx.getMessage}", sqlEx)
    }

  // ------------------------------------------------------------------------------------------------------
  // DASSdk interface
  // ------------------------------------------------------------------------------------------------------

  /**
   * @return a list of table definitions discovered by the backend.
   */
  override def tableDefinitions: Seq[TableDefinition] = tables.values.map(_.definition).toSeq

  /**
   * @return a list of function definitions. SQLite example returns empty by default.
   */
  override def functionDefinitions: Seq[FunctionDefinition] = Seq.empty

  /**
   * Retrieve a table by name (case-sensitive or lowercased, depending on how you stored them).
   */
  override def getTable(name: String): Option[DASTable] = tables.get(name)

  /**
   * Retrieve a function by name, if implemented. For now, none.
   */
  override def getFunction(name: String): Option[DASFunction] = None

  /**
   * Close the SDK and underlying connections.
   */
  override def close(): Unit = {
    dataSource.close()
  }

  /**
   * Helper that instantiates the HikariDataSource and unwraps pool init errors.
   */
  private def createDataSourceOrThrow(hc: HikariConfig, url: String): HikariDataSource = {
    try {
      new HikariDataSource(hc)
    } catch {
      case e: PoolInitializationException =>
        e.getCause match {
          case sqlEx: SQLException =>
            // Could be a bad path or locked file, etc.
            throw new DASSdkInvalidArgumentException(s"Could not connect: ${sqlEx.getMessage}", sqlEx)
          case other =>
            throw new RuntimeException(s"Unexpected error initializing connection: ${other.getMessage}", other)
        }
    }
  }
}
