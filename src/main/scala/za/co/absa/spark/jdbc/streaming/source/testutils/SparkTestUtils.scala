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

package za.co.absa.spark.jdbc.streaming.source.testutils

import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.apache.spark.sql.types.StructType

import reflect.runtime.universe.TypeTag

object SparkTestUtils {

  /**
    * Finds a Spark SQL schema for a given class.
    */
  def getSparkSchema[T <: Product]()(implicit tag: TypeTag[T]): StructType = {
    Encoders.product[T].schema
  }

  /**
    * Creates an empty DataFrame with the schema provided by a given class.
    */
  def getEmptyDataFrame[T <: Product](spark: SparkSession)(implicit tag: TypeTag[T]): DataFrame = {
    import spark.implicits._
    spark.createDataset(Seq[T]()).toDF()
  }

  /**
    * Reads a stream of data into a Spark memory table.
    *
    * It reads all the available data and then stops.
    *
    * IMPORTANT: Pay attention to the size of the table. This method should be used for tests, and not in production.
    *
    * @param spark SparkSession instance.
    * @param streamFormat Format of the input stream (e.g. Kafka, JDBC, etc).
    * @param tableName Name of the memory table where the data will be written.
    * @param options Options to be used by Spark to connect to the stream.
    * @return DataFrame containing the content of the table.
    */
  def readStreamIntoMemoryTable(spark: SparkSession, streamFormat: String, tableName: String,
                                options: Map[String,String]): DataFrame = {
    val stream = spark
      .readStream
      .format(streamFormat)
      .options(options)
      .load()

    val query = stream.
      writeStream
      .format(source = "memory")
      .queryName(tableName)
      .start()

    query.processAllAvailable()
    query.stop()

    spark.table(tableName)
  }
}
