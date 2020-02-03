/*
 * Copyright 2020 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.streaming.sources

import java.sql.Date
import java.time.LocalDate
import java.time.temporal.ChronoUnit
import java.util.{Random, UUID}

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.streaming.sources.JDBCStreamingSourceV1._
import org.scalatest.FunSuite
import utils.SparkJDBCUtils.{jdbcDefaultConnectionParams, writeToJDBC}
import utils.TestClass
import za.co.absa.spark.jdbc.streaming.source.testutils.SparkTestBase
import za.co.absa.spark.jdbc.streaming.source.testutils.SparkTestUtils._
import org.apache.spark.sql.functions.{min,max}

class TestIntegrationJDBCStreamingSourceV1 extends FunSuite with SparkTestBase {

  private val fullStreamingFormat = "za.co.absa.spark.jdbc.streaming.source.providers.JDBCStreamingSourceProviderV1"

  private val randGen = new Random()

  private def randomTestData(startDate: String, endDate: String): Seq[TestClass] = {
    val start = LocalDate.parse(startDate)
    val end = LocalDate.parse(endDate)

    (0 to ChronoUnit.DAYS.between(start, end).toInt).map(days => randomTestData(start.plusDays(days), randGen))
  }

  private def randomTestData(date: LocalDate, randGen: Random): TestClass = {
    TestClass(
      a = randGen.nextInt(),
      b = randGen.nextLong(),
      c = randGen.nextDouble(),
      d = java.sql.Date.valueOf(date),
      e = UUID.randomUUID().toString
    )
  }

  private def rowToTestClass(row: Row): TestClass = {
    TestClass(
      a = row.getAs[Int](fieldName = "a"),
      b = row.getAs[Long](fieldName = "b"),
      c = row.getAs[Double](fieldName = "c"),
      d = row.getAs[Date](fieldName = "d"),
      e = row.getAs[String](fieldName = "e")
    )
  }

  private def randomTableName: String = {
    s"data_${Math.abs(randGen.nextInt())}_${Math.abs(randGen.nextInt())}"
  }

  test(testName = "consume all available data and stop") {
    val tableName = randomTableName
    val offsetField = "d"
    val params = Map(CONFIG_OFFSET_FIELD -> offsetField) ++ jdbcDefaultConnectionParams(tableName)

    val expected = randomTestData(startDate = "2020-01-01", endDate = "2020-01-05")
    writeToJDBC[TestClass](spark, params, expected)

    val batchData = readStreamIntoMemoryTable(spark, fullStreamingFormat, tableName, params)

    val actual = batchData.collect().map(rowToTestClass)

    assert(actual.toSet == expected.toSet)
  }

  test(testName = "get start offset from parameters") {
    val seriesStartDate = "2020-01-01"
    val expectedStartDate = "2020-01-03"

    val tableName = randomTableName
    val offsetField = "d"
    val params = Map(CONFIG_OFFSET_FIELD -> offsetField,
      CONFIG_START_OFFSET -> expectedStartDate) ++ jdbcDefaultConnectionParams(tableName)

    val randomData = randomTestData(startDate = seriesStartDate, endDate = "2020-01-05")
    writeToJDBC[TestClass](spark, params, randomData)

    val batchData = readStreamIntoMemoryTable(spark, fullStreamingFormat, tableName, params)

    val actualStartDate = batchData.select(min(offsetField)).first().get(0).toString

    assert(actualStartDate == expectedStartDate)
  }
}
