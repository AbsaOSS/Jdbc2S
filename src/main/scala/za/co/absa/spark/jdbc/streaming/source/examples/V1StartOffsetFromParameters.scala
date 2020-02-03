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

package za.co.absa.spark.jdbc.streaming.source.examples

import java.sql.Date
import java.time.LocalDate
import java.time.temporal.ChronoUnit
import java.util.{Random, UUID}

import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{SaveMode, SparkSession}

private case class Ride(user: String, value: Double, date: Date)

/**
  * Uses a date field as the offset tracker and gets the start offset from the list of parameters.
  */
object V1StartOffsetFromParameters {

  private lazy val randGen = new Random()

  private lazy val spark = SparkSession
    .builder()
    .appName(name = "test-jdbc-streaming-v1")
    .master(master = "local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  private lazy val jdbcOptions = {
    Map(
      "user" -> "a_user",
      "password" -> "a_password",
      "database" -> "h2_db",
      "driver" -> "org.h2.Driver",
      "url" -> "jdbc:h2:mem:myDb;DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=false",
      "dbtable" -> "transactions"
    )
  }

  def main(args: Array[String]): Unit = {

    val format = "za.co.absa.spark.jdbc.streaming.source.providers.JDBCStreamingSourceProviderV1"

    val startDate = "2020-01-01"
    val endDate = "2020-01-10"
    val desiredStart = "2020-01-05"

    // writes first batch of random data to startDate to endDate
    val transactions = randomRides(startDate, endDate)
    writeTransactions(transactions)

    // the offset is 'date', which will be treated as a string
    readAndShowTransactions(format = format, offsetField = "date", desiredStart, timeoutMs = 10000L)
  }

  private def writeTransactions(transactions: Seq[Ride]): Unit = {
    import spark.implicits._
    val expected = spark.createDataset(transactions).toDF()
    expected
      .write
      .mode(SaveMode.Append)
      .format(source = "jdbc")
      .options(jdbcOptions)
      .save()
  }

  private def readAndShowTransactions(format: String, offsetField: String, desiredStart: String,
                                      timeoutMs: Long): Unit = {

    import org.apache.spark.sql.execution.streaming.sources.JDBCStreamingSourceV1.{CONFIG_OFFSET_FIELD,
      CONFIG_START_OFFSET}

    val params = jdbcOptions + (CONFIG_OFFSET_FIELD -> offsetField) + (CONFIG_START_OFFSET -> desiredStart)

    val stream = spark.readStream
      .format(format)
      .options(params)
      .load

    val query = stream
      .writeStream
      .format(source = "console")
      .outputMode(OutputMode.Append())
      .start()

    query.awaitTermination(timeoutMs = timeoutMs)
    query.stop()
  }

  private def randomRides(startDate: String, endDate: String): Seq[Ride] = {
    val start = LocalDate.parse(startDate)
    val end = LocalDate.parse(endDate)

    (0 to ChronoUnit.DAYS.between(start, end).toInt).map(d => randomRide(start.plusDays(d)))
  }

  private def randomRide(date: LocalDate): Ride = {
    Ride(UUID.randomUUID().toString, randGen.nextDouble(), Date.valueOf(date))
  }
}
