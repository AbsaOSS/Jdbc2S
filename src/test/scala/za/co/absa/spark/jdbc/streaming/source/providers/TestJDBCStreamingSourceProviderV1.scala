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

package za.co.absa.spark.jdbc.streaming.source.providers

import java.sql.Date
import java.time.{LocalDate, Period}
import java.util.{Random, UUID}

import org.apache.spark.sql.types.{LongType, StringType, StructType}
import org.scalatest.{BeforeAndAfterAll, FunSpec}
import utils.SparkTestUtils._
import org.apache.spark.sql.execution.streaming.sources.JDBCStreamingSourceV1
import utils.SparkJDBCUtils._
import utils.{SparkTestBase, TestClass}
import utils.SparkSchemaUtils._
import JDBCStreamingSourceV1._

class TestJDBCStreamingSourceProviderV1 extends FunSpec with BeforeAndAfterAll with SparkTestBase {

  private lazy val jdbcOptions = jdbcDefaultConnectionParams(tableName = "data")

  override def beforeAll(): Unit = {
    val data = getTestInstances(howMany = 5)
    writeToJDBC(spark, jdbcOptions, data)
  }

  describe(description = "sourceSchema()") {
    it("should return schema provider name the same as short name") {
      val provider = new JDBCStreamingSourceProviderV1()
      val (sourceName, _) = provider.sourceSchema(spark.sqlContext, None, provider.shortName(), jdbcOptions)

      assert(sourceName == provider.shortName())
    }

    it("it should use schema from batch dataframe") {
      val provider = new JDBCStreamingSourceProviderV1()
      val (_, actualSchema) = provider.sourceSchema(spark.sqlContext, None, provider.shortName(), jdbcOptions)

      val expectedSchema = getSparkSchema[TestClass]

      assert(equalsIgnoreNullability(actualSchema, expectedSchema))
    }

    it("should ignore provided schema") {
      val schemaToBeIgnored = new StructType()
        .add(name = "f1", LongType)
        .add(name = "f2", StringType)

      val provider = new JDBCStreamingSourceProviderV1()
      val (_, actualSchema) = provider.sourceSchema(spark.sqlContext, None, provider.shortName(), jdbcOptions)

      assert(actualSchema.length != schemaToBeIgnored.length)
    }
  }

  describe(description = "createSource()") {
    it("should return an instance of the correct source if correct provider name") {
      val provider = new JDBCStreamingSourceProviderV1()
      val params = Map(CONFIG_OFFSET_FIELD -> "d") ++ jdbcOptions
      val source = provider.createSource(spark.sqlContext, "/", None, provider.shortName(), params)

      assert(source.isInstanceOf[JDBCStreamingSourceV1])
    }

    it("should passes parameters, provider name and batch dataframe to source") {
      val expectedMetadata = "/tmp/"
      val expectedParams = jdbcOptions ++ Map(CONFIG_OFFSET_FIELD -> "d")
      val expectedSchema = getSparkSchema[TestClass]

      val provider = new JDBCStreamingSourceProviderV1()
      val source = provider
        .createSource(spark.sqlContext, expectedMetadata, None, provider.shortName(), expectedParams)
        .asInstanceOf[JDBCStreamingSourceV1]

      assert(source.parameters.toSeq == expectedParams.toSeq)
      assert(source.providerName == provider.shortName())
      assert(equalsIgnoreNullability(source.schema, expectedSchema))
    }

    it("should throw on getting schema if incorrect provider is passed") {
      val provider = new JDBCStreamingSourceProviderV1()
      assertThrows[IllegalArgumentException](provider.sourceSchema(spark.sqlContext, None,
        providerName = "incorrect", jdbcOptions))
    }

    it("should use DataFrame schema and not the informed one") {
      val ignoredSchema = new StructType().add(name = "f1", LongType).add(name = "f2", StringType)

      val params = Map(CONFIG_OFFSET_FIELD -> "d") ++ jdbcOptions

      val provider = new JDBCStreamingSourceProviderV1()
      val source = provider.createSource(spark.sqlContext, metadataPath = "/tmp", Some(ignoredSchema),
        provider.shortName(), params)

      val expectedSchema = getSparkSchema[TestClass]()

      assert(equalsIgnoreNullability(source.schema, expectedSchema))
    }

    it("should create sources with enabled streaming by default") {
      val ignoredSchema = new StructType().add(name = "f1", LongType).add(name = "f2", StringType)

      val params = Map(CONFIG_OFFSET_FIELD -> "d") ++ jdbcOptions

      val provider = new JDBCStreamingSourceProviderV1()
      val source = provider.createSource(spark.sqlContext, metadataPath = "/tmp", Some(ignoredSchema),
        provider.shortName(), params)
        .asInstanceOf[JDBCStreamingSourceV1]

      assert(source.streamingEnabled)
    }

    it("should create sources with disabled streaming if told to") {
      val ignoredSchema = new StructType().add(name = "f1", LongType).add(name = "f2", StringType)

      val params = Map(CONFIG_OFFSET_FIELD -> "d") ++ jdbcOptions

      val provider = new JDBCStreamingSourceProviderV1()
      val source = provider.createSourceWithDisabledStreaming(spark.sqlContext, metadataPath = "/tmp",
        Some(ignoredSchema), provider.shortName(), params)
        .asInstanceOf[JDBCStreamingSourceV1]

      assert(!source.streamingEnabled)
    }
  }


  private def getTestInstances(howMany: Int): Seq[TestClass] = {
    val randGen = new Random()
    (0 to howMany)
      .map(_ => getTestInstance(randGen))
  }

  private def getTestInstance(randGen: Random): TestClass = {
    TestClass(
      a = randGen.nextInt(),
      b = randGen.nextLong(),
      c = randGen.nextDouble(),
      d = randomDate(randGen),
      e = UUID.randomUUID().toString
    )
  }

  private def randomDate(randGen: Random): Date = {
    val date = LocalDate
      .now()
      .minus(Period.ofDays(randGen.nextInt(365 * 70)))

    Date.valueOf(date)
  }
}
