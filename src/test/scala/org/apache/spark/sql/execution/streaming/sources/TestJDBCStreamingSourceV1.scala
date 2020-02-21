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

import java.time.LocalDate
import java.time.temporal.ChronoUnit
import java.util.{Random, UUID}

import org.scalatest.{BeforeAndAfterAll, FunSpec}
import JDBCStreamingSourceV1._
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.execution.streaming.{Offset, SerializedOffset}
import org.apache.spark.sql.functions.{max, min}
import utils.SparkJDBCUtils._
import utils.{SparkTestBase, TestClass}
import za.co.absa.spark.jdbc.streaming.source.offsets.{JDBCSingleFieldOffset, OffsetField, OffsetRange}
import za.co.absa.spark.jdbc.streaming.source.providers.JDBCStreamingSourceProviderV1
import utils.SparkTestUtils._
import utils.SparkSchemaUtils._
import utils.SparkSchemaUtils.equalsIgnoreNullability
import za.co.absa.spark.jdbc.streaming.source.offsets.JsonOffsetMapper

class TestJDBCStreamingSourceV1 extends FunSpec with BeforeAndAfterAll with SparkTestBase {

  private val fullStreamingFormat = "za.co.absa.spark.jdbc.streaming.source.providers.JDBCStreamingSourceProviderV1"

  import spark.implicits._

  private val randGen = new Random()

  private def writeRandomTestData(expectedStartOffset: String, expectedEndOffset: String,
                                  jdbcOptions: Map[String,String]): Unit = {
    val data = randomTestData(expectedStartOffset, expectedEndOffset)
    writeToJDBC[TestClass](spark, jdbcOptions, data)
  }

  private def writeRandomTestData(startInt: Int, endInt: Int, jdbcOptions: Map[String,String]): Unit = {
    val data = randomTestData(startInt, endInt)
    writeToJDBC[TestClass](spark, jdbcOptions, data)
  }

  private def writeRandomTestData(startDouble: Double, endDouble: Double, step: Double,
                                  jdbcOptions: Map[String,String]): Unit = {
    val data = randomTestData(startDouble, endDouble, step)
    writeToJDBC[TestClass](spark, jdbcOptions, data)
  }

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

  private def randomTestData(a: Int, date: String, randGen: Random): TestClass = {
    TestClass(
      a = a,
      b = randGen.nextLong(),
      c = randGen.nextDouble(),
      d = java.sql.Date.valueOf(date),
      e = UUID.randomUUID().toString
    )
  }

  private def randomTestData(c: Double, date: String, randGen: Random): TestClass = {
    TestClass(
      a = randGen.nextInt(),
      b = randGen.nextLong(),
      c = c,
      d = java.sql.Date.valueOf(date),
      e = UUID.randomUUID().toString
    )
  }

  private def randomTestData(startInt: Int, endInt: Int): Seq[TestClass] = {
    val randomDate = "2020-01-01"
    (startInt to endInt).map(a => randomTestData(a, randomDate, randGen))
  }

  private def randomTestData(startDouble: Double, endDouble: Double, step: Double): Seq[TestClass] = {
    val randomDate = "2020-01-01"
    (startDouble to endDouble by step).map(c => randomTestData(c, randomDate, randGen))
  }

  private def equalOffsets(retrievedOffset: Option[Offset], expectedStart: String, expectedEnd: String): Boolean = {
    (retrievedOffset: @unchecked) match {
      case Some(offset: JDBCSingleFieldOffset) =>
        offset.fieldsOffsets.range.start.get == expectedStart && offset.fieldsOffsets.range.end.get == expectedEnd
      case _ => false
    }
  }

  private def equalOffsets(offset1: JDBCSingleFieldOffset, offset2: JDBCSingleFieldOffset): Boolean = {
    offset1.fieldsOffsets.fieldName == offset2.fieldsOffsets.fieldName &&
      offset1.fieldsOffsets.range == offset2.fieldsOffsets.range
  }

  private def randomTableName: String = {
    s"data_${Math.abs(randGen.nextInt())}_${Math.abs(randGen.nextInt())}"
  }

  private def toOffsetRange(fieldName: String, start: String, end: String): JDBCSingleFieldOffset = {
    toOffsetRange(fieldName, Some(start), Some(end))
  }

  private def toOffsetRange(fieldName: String, start: Option[String], end: Option[String]): JDBCSingleFieldOffset = {
    val range = OffsetRange(start, end)
    JDBCSingleFieldOffset(OffsetField(fieldName, range))
  }

  describe(description = "constructor") {

    it("should throw if offset field is not informed") {
      val params = Map(JDBCOptions.JDBC_TABLE_NAME -> "table")
      assertThrows[IllegalArgumentException](new JDBCStreamingSourceV1(spark.sqlContext,
        providerName="whatever", params, metadataPath = "/tmp", getSparkSchema[TestClass]))
    }

    it("should throw if dataframe is null") {
      val params = Map(CONFIG_OFFSET_FIELD -> "d", CONFIG_OFFSET_FIELD_DATE_FORMAT -> "YYYY-MM-DD")

      assertThrows[IllegalArgumentException](new JDBCStreamingSourceV1(spark.sqlContext,
        providerName="whatever", params, metadataPath = "/tmp", null))
    }

    it("should throw if table name is not informed") {
      val params = Map(CONFIG_OFFSET_FIELD -> "d")

      assertThrows[IllegalArgumentException](new JDBCStreamingSourceV1(spark.sqlContext,
        providerName="whatever", params, metadataPath = "/tmp", getSparkSchema[TestClass]))
    }

    it("should throw if offset field is date and format is not specified") {
      val params = Map(CONFIG_OFFSET_FIELD -> "d", JDBCOptions.JDBC_TABLE_NAME -> "table")

      assertThrows[IllegalArgumentException](new JDBCStreamingSourceV1(spark.sqlContext,
        providerName="whatever", params, metadataPath = "/tmp", getSparkSchema[TestClass]))
    }

    it("should not throw if offset field is not date and date format is not specified") {
      val params = Map(CONFIG_OFFSET_FIELD -> "a", JDBCOptions.JDBC_TABLE_NAME -> "table")

      new JDBCStreamingSourceV1(spark.sqlContext,
        providerName="whatever", params, metadataPath = "/tmp", getSparkSchema[TestClass])

      succeed
    }
  }

  describe(description = "schema") {

    it("should return the schema received as an argument") {
      val params = Map(CONFIG_OFFSET_FIELD -> "a", JDBCOptions.JDBC_TABLE_NAME -> "table")

      val source = new JDBCStreamingSourceV1(spark.sqlContext, providerName="whatever", params,
        metadataPath = "/tmp", getSparkSchema[TestClass])

      val expectedSchema = getSparkSchema[TestClass]()

      assert(equalsIgnoreNullability(source.schema, expectedSchema))
    }
  }

  describe(description = "getOffset") {

    it("should use offset field provided in params") {
      val expectedOffsetField = "d"
      val expectedStartOffset = "2020-01-01"
      val expectedEndOffset = "2020-01-05"

      val params = Map(CONFIG_OFFSET_FIELD -> expectedOffsetField,
        CONFIG_START_OFFSET -> expectedStartOffset,
        CONFIG_OFFSET_FIELD_DATE_FORMAT -> "YYYY-MM-DD") ++ jdbcDefaultConnectionParams(randomTableName)

      writeRandomTestData(expectedStartOffset, expectedEndOffset, params)

      val source = new JDBCStreamingSourceV1(spark.sqlContext, providerName="whatever", params,
        metadataPath = "/tmp", getSparkSchema[TestClass])

      (source.getOffset: @unchecked) match {
        case Some(offset: JDBCSingleFieldOffset) => assert(offset.fieldsOffsets.fieldName == expectedOffsetField)
        case _ => fail(message = "Did not use offset field name from parameters")
      }
    }

    it("should get start offset from parameters when available") {
      val expectedOffsetField = "d"
      val actualStartOffset = "2020-01-01"
      val expectedStartOffset = "2020-01-03"
      val expectedEndOffset = "2020-01-05"

      val params = Map(CONFIG_OFFSET_FIELD -> expectedOffsetField,
        CONFIG_OFFSET_FIELD_DATE_FORMAT -> "YYYY-MM-DD",
        CONFIG_START_OFFSET -> expectedStartOffset) ++ jdbcDefaultConnectionParams(randomTableName)

      writeRandomTestData(actualStartOffset, expectedEndOffset, params)

      val source = new JDBCStreamingSourceV1(spark.sqlContext, providerName="whatever", params,
        metadataPath = "/tmp", getSparkSchema[TestClass])

      (source.getOffset: @unchecked) match {
        case Some(offset: JDBCSingleFieldOffset) => assert(offset.fieldsOffsets.range.start.get == expectedStartOffset)
        case _ => fail(message = "Did not use start offset from parameters")
      }
    }

    it("should get start offset from data if not in parameters") {
      val expectedStartOffset = "2020-01-01"
      val expectedEndOffset = "2020-01-05"

      val expectedOffsetField = "d"
      val params = Map(CONFIG_OFFSET_FIELD -> expectedOffsetField,
        CONFIG_OFFSET_FIELD_DATE_FORMAT -> "YYYY-MM-DD") ++ jdbcDefaultConnectionParams(randomTableName)

      writeRandomTestData(expectedStartOffset, expectedEndOffset, params)

      val source = new JDBCStreamingSourceV1(spark.sqlContext, providerName="whatever", params,
        metadataPath = "/tmp", getSparkSchema[TestClass])

      (source.getOffset: @unchecked) match {
        case Some(offset: JDBCSingleFieldOffset) => assert(offset.fieldsOffsets.range.start.get == expectedStartOffset)
        case None => fail(message = "Did not retrieve start offset from data")
      }
    }

    it("should get end offset from data") {
      val expectedStartOffset = "2020-01-01"
      val expectedEndOffset = "2020-01-05"

      val expectedOffsetField = "d"
      val params = Map(CONFIG_OFFSET_FIELD -> expectedOffsetField,
        CONFIG_START_OFFSET -> expectedStartOffset,
        CONFIG_OFFSET_FIELD_DATE_FORMAT -> "YYYY-MM-DD") ++ jdbcDefaultConnectionParams(randomTableName)

      writeRandomTestData(expectedStartOffset, expectedEndOffset, params)

      val source = new JDBCStreamingSourceV1(spark.sqlContext, providerName="whatever", params,
        metadataPath = "/tmp", getSparkSchema[TestClass])

      val endOffset = source.getOffset
      (endOffset: @unchecked) match {
        case Some(offset: JDBCSingleFieldOffset) => assert(offset.fieldsOffsets.range.end.get == expectedEndOffset)
        case None => fail(message = "Did not retrieve end offset from data")
      }
    }

    it("should advance offsets if more data is available") {
      val tableName = randomTableName
      val expectedOffsetField = "d"
      val params = Map(CONFIG_OFFSET_FIELD -> expectedOffsetField,
      CONFIG_OFFSET_FIELD_DATE_FORMAT -> "YYYY-MM-DD") ++ jdbcDefaultConnectionParams(tableName)

      val expectedStartOffset1 = "2020-01-01"
      val expectedEndOffset1 = "2020-01-05"
      writeRandomTestData(expectedStartOffset1, expectedEndOffset1, params)

      val provider = new JDBCStreamingSourceProviderV1()
      val source = provider.createSource(spark.sqlContext, metadataPath="/tmp", None, provider.shortName(), params)

      assert(equalOffsets(source.getOffset, expectedStartOffset1, expectedEndOffset1))

      // advances offsets
      val expectedStartOffset2 = "2020-01-05"
      val expectedEndOffset2 = "2020-01-10"
      writeRandomTestData(expectedStartOffset2, expectedEndOffset2, params)

      assert(equalOffsets(source.getOffset, expectedStartOffset2, expectedEndOffset2))
    }

    it("should return empty offset if no new data is available") {
      val tableName = randomTableName
      val expectedOffsetField = "d"
      val params = Map(CONFIG_OFFSET_FIELD -> expectedOffsetField,
        CONFIG_OFFSET_FIELD_DATE_FORMAT -> "YYYY-MM-DD") ++ jdbcDefaultConnectionParams(tableName)

      val expectedStartOffset1 = "2020-01-01"
      val expectedEndOffset1 = "2020-01-05"
      writeRandomTestData(expectedStartOffset1, expectedEndOffset1, params)

      val provider = new JDBCStreamingSourceProviderV1()
      val source = provider.createSource(spark.sqlContext, metadataPath="/tmp", None, provider.shortName(), params)

      assert(equalOffsets(source.getOffset, expectedStartOffset1, expectedEndOffset1))

      // another call should return empty, since no new data were written
      assert(source.getOffset.isEmpty)
    }

    it("should return empty until new data is available") {
      val tableName = randomTableName
      val expectedOffsetField = "d"
      val params = Map(CONFIG_OFFSET_FIELD -> expectedOffsetField,
        CONFIG_OFFSET_FIELD_DATE_FORMAT -> "YYYY-MM-DD") ++ jdbcDefaultConnectionParams(tableName)

      val expectedStartOffset1 = "2020-01-01"
      val expectedEndOffset1 = "2020-01-05"
      writeRandomTestData(expectedStartOffset1, expectedEndOffset1, params)

      val provider = new JDBCStreamingSourceProviderV1()
      val source = provider.createSource(spark.sqlContext, metadataPath="/tmp", None, provider.shortName(), params)

      assert(equalOffsets(source.getOffset, expectedStartOffset1, expectedEndOffset1))

      // next calls should return empty
      assert(source.getOffset.isEmpty)
      assert(source.getOffset.isEmpty)
      assert(source.getOffset.isEmpty)

      val expectedStartOffset2 = "2020-01-05"
      val expectedEndOffset2 = "2020-01-10"
      writeRandomTestData(expectedStartOffset2, expectedEndOffset2, params)

      assert(equalOffsets(source.getOffset, expectedStartOffset2, expectedEndOffset2))
    }
  }

  describe(description = "getBatch") {

    it("should get the end offset from 'end.end' argument") {
      val tableName = randomTableName
      val expectedOffsetField = "d"
      val params = Map(CONFIG_OFFSET_FIELD -> expectedOffsetField,
        CONFIG_OFFSET_FIELD_DATE_FORMAT -> "YYYY-MM-DD") ++ jdbcDefaultConnectionParams(tableName)

      val expectedStartOffset = "2020-01-01"
      val expectedEndOffset = "2020-01-05"

      writeRandomTestData(expectedStartOffset, expectedEndOffset, params)

      val batch = readStreamIntoMemoryTable(spark, fullStreamingFormat, tableName, params)

      val actualEndOffset = batch.select(max(expectedOffsetField)).first().get(0).toString

      assert(actualEndOffset == expectedEndOffset)
    }

    it("should set the start offset from 'end.start' argument if 'start' argument is empty") {
      val tableName = randomTableName
      val expectedOffsetField = "d"
      val params = Map(CONFIG_OFFSET_FIELD -> expectedOffsetField,
        CONFIG_OFFSET_FIELD_DATE_FORMAT -> "YYYY-MM-DD") ++ jdbcDefaultConnectionParams(tableName)

      val expectedStartOffset = "2020-02-01"
      val expectedEndOffset = "2020-02-05"
      writeRandomTestData(expectedStartOffset, expectedEndOffset, params)

      val batch = readStreamIntoMemoryTable(spark, fullStreamingFormat, tableName, params)

      val actualStartOffset = batch.select(min(expectedOffsetField)).first().get(0).toString

      assert(actualStartOffset == expectedStartOffset)
    }

    it("should set the start offset to 'start.end' exclusive if start is defined") {
      val tableName = randomTableName
      val expectedOffsetField = "d"
      val params = Map(CONFIG_OFFSET_FIELD -> expectedOffsetField,
        CONFIG_OFFSET_FIELD_DATE_FORMAT -> "YYYY-MM-DD") ++ jdbcDefaultConnectionParams(tableName)

      val previousBatchStartOffset = "2020-03-01"
      val previousBatchEndOffset = "2020-03-08"

      val expectedStartOffset = "2020-03-09"
      val expectedEndOffset = "2020-04-20"
      writeRandomTestData(previousBatchStartOffset, expectedEndOffset, params)

      val provider = new JDBCStreamingSourceProviderV1()
      val source = provider.createSourceWithDisabledStreaming(spark.sqlContext, metadataPath="/tmp", None,
        provider.shortName(), params)

      val startOffset = toOffsetRange(expectedOffsetField, previousBatchStartOffset, previousBatchEndOffset)

      val endOffset = toOffsetRange(expectedOffsetField, expectedStartOffset, expectedEndOffset)

      val batch = source.getBatch(Some(startOffset), endOffset)

      val actualStartOffset = batch.select(min(expectedOffsetField)).first().get(0).toString
      val actualEndOffset = batch.select(max(expectedOffsetField)).first().get(0).toString

      assert(actualStartOffset == expectedStartOffset)
      assert(actualEndOffset == expectedEndOffset)
    }

    it("should retrieve return multiple batches if data is available") {
      val tableName = randomTableName
      val offsetField = "d"
      val params = Map(CONFIG_OFFSET_FIELD -> offsetField,
        CONFIG_OFFSET_FIELD_DATE_FORMAT -> "YYYY-MM-DD") ++ jdbcDefaultConnectionParams(tableName)

      val earliestDate = "2020-01-01"
      val latestDate = "2020-01-20"

      // writes all the records
      writeRandomTestData(earliestDate, latestDate, params)

      val provider = new JDBCStreamingSourceProviderV1()
      val source = provider.createSourceWithDisabledStreaming(spark.sqlContext, metadataPath="/tmp", None,
        provider.shortName(), params)

      // creates micro-batches based on date ranges
      // the previous entry will contain the exclusive start offset except for the first iteration
      val ranges = Seq(
        None,
        Some(toOffsetRange(offsetField, earliestDate, "2020-01-03")),
        Some(toOffsetRange(offsetField, "2020-01-04", "2020-01-08")),
        Some(toOffsetRange(offsetField, "2020-01-09", "2020-01-15")),
        Some(toOffsetRange(offsetField, "2020-01-16",latestDate )))

      // for each pair of dates, starting from None start, gets the batch related data
      ranges.sliding(size = 2, step = 1).foreach(pair => {
        val start = pair.head
        val end = pair.tail.head.get

        import end.fieldsOffsets.range

        val (expectedStartOffset, expectedEndOffset) = (range.start.get, range.end.get)

        val batch = source.getBatch(start, end)

        val actualStartOffset = batch.select(min(offsetField)).first().get(0).toString
        val actualEndOffset = batch.select(max(offsetField)).first().get(0).toString

        assert(actualStartOffset == expectedStartOffset)
        assert(actualEndOffset == expectedEndOffset)
      })
    }

    it("should retrieve all the fields") {
      val tableName = randomTableName
      val expectedOffsetField = "d"
      val params = Map(CONFIG_OFFSET_FIELD -> expectedOffsetField,
        CONFIG_OFFSET_FIELD_DATE_FORMAT -> "YYYY-MM-DD") ++ jdbcDefaultConnectionParams(tableName)

      val expectedStartOffset = "2020-01-01"
      val expectedEndOffset = "2020-01-05"
      writeRandomTestData(expectedStartOffset, expectedEndOffset, params)

      val batch = readStreamIntoMemoryTable(spark, fullStreamingFormat, tableName, params)

      val actualSchema = batch.schema

      val expectedSchema = getSparkSchema[TestClass]()

      // since no field was selected, the schema from the original case class should be the same
      // as the dataframe's
      assert(equalsIgnoreNullability(actualSchema, expectedSchema))
    }

    it("should accept int type as offset") {
      val tableName = randomTableName
      val offsetField = "a"
      val params = Map(CONFIG_OFFSET_FIELD -> offsetField) ++ jdbcDefaultConnectionParams(tableName)

      val earliestInt = "1"
      val latestInt = "20"

      // writes all the records
      writeRandomTestData(earliestInt.toInt, latestInt.toInt, params)

      val provider = new JDBCStreamingSourceProviderV1()
      val source = provider.createSourceWithDisabledStreaming(spark.sqlContext, metadataPath="/tmp", None,
        provider.shortName(), params)

      // creates micro-batches based on integer ranges
      // the previous entry will contain the exclusive start offset except for the first iteration
      val ranges = Seq(
        None,
        Some(toOffsetRange(offsetField, earliestInt, "3")),
        Some(toOffsetRange(offsetField, "4", "8")),
        Some(toOffsetRange(offsetField, "9", "15")),
        Some(toOffsetRange(offsetField, "16", latestInt)))

      // for each pair of dates, starting from None start, gets the batch related data
      ranges.sliding(size = 2, step = 1).foreach(pair => {
        val start = pair.head
        val end = pair.tail.head.get

        import end.fieldsOffsets.range

        val (expectedStartOffset, expectedEndOffset) = (range.start.get, range.end.get)

        val batch = source.getBatch(start, end)

        val actualStartOffset = batch.select(min(offsetField)).first().get(0).toString
        val actualEndOffset = batch.select(max(offsetField)).first().get(0).toString

        assert(actualStartOffset.toInt == expectedStartOffset.toInt)
        assert(actualEndOffset.toInt == expectedEndOffset.toInt)
      })
    }

    it("should accept double type as offset") {
      val tableName = randomTableName
      val offsetField = "c"
      val params = Map(CONFIG_OFFSET_FIELD -> offsetField) ++ jdbcDefaultConnectionParams(tableName)

      val earliestDouble = "0.5"
      val latestDouble = "12.5"
      val step = 0.5d

      // writes all the records
      writeRandomTestData(earliestDouble.toDouble, latestDouble.toDouble, step, params)

      val provider = new JDBCStreamingSourceProviderV1()
      val source = provider.createSourceWithDisabledStreaming(spark.sqlContext, metadataPath="/tmp", None,
        provider.shortName(), params)

      // creates micro-batches based on double ranges
      // the previous entry will contain the exclusive start offset except for the first iteration
      val ranges = Seq(
        None,
        Some(toOffsetRange(offsetField, earliestDouble, "2")),
        Some(toOffsetRange(offsetField, "2.5", "4.5")),
        Some(toOffsetRange(offsetField, "5", "9.5")),
        Some(toOffsetRange(offsetField, "10", latestDouble)))

      // for each pair of dates, starting from None start, gets the batch related data
      ranges.sliding(size = 2, step = 1).foreach(pair => {
        val start = pair.head
        val end = pair.tail.head.get

        import end.fieldsOffsets.range

        val (expectedStartOffset, expectedEndOffset) = (range.start.get, range.end.get)

        val batch = source.getBatch(start, end)

        val actualStartOffset = batch.select(min(offsetField)).first().get(0).toString
        val actualEndOffset = batch.select(max(offsetField)).first().get(0).toString

        assert(actualStartOffset.toDouble == expectedStartOffset.toDouble)
        assert(actualEndOffset.toDouble == expectedEndOffset.toDouble)
      })
    }
      // check [[org.apache.spark.sql.execution.streaming.MicroBatchExecution#populateStartOffsets]] to understand
    it("should return empty if being invoked for first time with offsets from checkpoint") {
      val offsetField = "d"
      val startOffset = "2020-01-01"
      val endOffset = "2020-01-05"

      val params = Map(CONFIG_OFFSET_FIELD -> offsetField,
        CONFIG_START_OFFSET -> startOffset,
        CONFIG_OFFSET_FIELD_DATE_FORMAT -> "YYYY-MM-DD") ++ jdbcDefaultConnectionParams(randomTableName)

      writeRandomTestData(startOffset, endOffset, params)

      val source = new JDBCStreamingSourceV1(spark.sqlContext, providerName="whatever", params,
        metadataPath = "/tmp", getSparkSchema[TestClass], streamingEnabled = false)

      // at this point there is data, however, we're assuming the query is being restarted from offset, so, the data
      // should only by identified after invoking '.getOffset'

      val offset = toOffsetRange(offsetField, startOffset, endOffset)
      import za.co.absa.spark.jdbc.streaming.source.offsets.JsonOffsetMapper

      val checkpointedOffset = SerializedOffset(JsonOffsetMapper.toJson(offset))

      assert(source.getBatch(None, checkpointedOffset).isEmpty)
    }

    // to avoid duplicates
    it("should set the current offset as the checkpointed one in the first call") {
      val offsetField = "d"
      val lastStartOffset = "2020-01-01"
      val lastEndOffset = "2020-01-05"

      val params = Map(CONFIG_OFFSET_FIELD -> offsetField,
        CONFIG_OFFSET_FIELD_DATE_FORMAT -> "YYYY-MM-DD") ++ jdbcDefaultConnectionParams(randomTableName)

      writeRandomTestData(lastStartOffset, lastEndOffset, params)

      val source = new JDBCStreamingSourceV1(spark.sqlContext, providerName="whatever", params,
        metadataPath = "/tmp", getSparkSchema[TestClass], streamingEnabled = false)

      // at this point there is data, however, we're assuming the query is being restarted from offset, so, the data
      // should only by identified after invoking '.getOffset'
      val checkpointedOffset = toOffsetRange(offsetField, lastStartOffset, lastEndOffset)
      val deserializedOffset = SerializedOffset(JsonOffsetMapper.toJson(checkpointedOffset))

      // since it is an instance of SerializedOffset, it should be ignored by the batch but yet be set as the latest
      // offset used. It is like restoring the state.
      source.getBatch(None, deserializedOffset)
      assert(equalOffsets(source.getLastOffset.get, checkpointedOffset))
    }

    it(s"should return empty if starting from checkpointed offset and no new data is available") {

      val offsetField = "d"
      val lastStartOffset = "2020-01-01"
      val lastEndOffset = "2020-01-05"

      val params = Map(CONFIG_OFFSET_FIELD -> offsetField,
        CONFIG_OFFSET_FIELD_DATE_FORMAT -> "YYYY-MM-DD") ++ jdbcDefaultConnectionParams(randomTableName)

      writeRandomTestData(lastStartOffset, lastEndOffset, params)

      val source = new JDBCStreamingSourceV1(spark.sqlContext, providerName="whatever", params,
        metadataPath = "/tmp", getSparkSchema[TestClass], streamingEnabled = false)

      // at this point there is data, however, we're assuming the query is being restarted from offset, so, the data
      // should only by identified after invoking '.getOffset'
      val checkpointedOffset = toOffsetRange(offsetField, lastStartOffset, lastEndOffset)
      val deserializedOffset = SerializedOffset(JsonOffsetMapper.toJson(checkpointedOffset))

      // just to set the current offset as the last one, i.e. the one coming from the checkpoint location
      val batch = source.getBatch(None, deserializedOffset)

      // since no new data is available, the current offset should be set to the previous one, from checkpoint
      val expectedResolvedOffset = JDBCSingleFieldOffset(OffsetField(offsetField,
        OffsetRange(Some(lastStartOffset), Some(lastEndOffset))))

      assert(source.getLastOffset.get == expectedResolvedOffset)
      assert(batch.isEmpty)
    }
  }
}
