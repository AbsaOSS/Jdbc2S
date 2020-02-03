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

import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import java.util.concurrent._

private case class Transaction(user: String, value: Double, date: Date)

object V1StartOffsetFromData {

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

  /**
    * Generates some random data of format [[Transaction]] from start to end date.
    */
  private class RandomDataSaver(start: String, end: String) extends Runnable {
    override def run(): Unit = {
      println(s"Inserting random data from $start to $end ...")
      writeTransactions(randomTransactions(start, end))
      println(s"Done")
    }
  }

  def main(args: Array[String]): Unit = {

    val format = "za.co.absa.spark.jdbc.streaming.source.providers.JDBCStreamingSourceProviderV1"

    val startDate = "2020-01-01"
    val endDate = "2020-01-10"

    // writes first batch of random data to startDate to endDate
    val transactions = randomTransactions(startDate, endDate)
    writeTransactions(transactions)

    val scheduler = new ScheduledThreadPoolExecutor(1)
    scheduler.schedule(new RandomDataSaver("2020-01-10", "2020-01-20"), 8, TimeUnit.SECONDS)
    scheduler.schedule(new RandomDataSaver("2020-01-20", "2020-01-30"), 16, TimeUnit.SECONDS)

    // the offset field will be 'date', which will be treated as string
    readAndShowTransactions(format = format, offsetField = "date", triggerMs = 4000L, timeoutMs = 30000L)
  }

  private def writeTransactions(transactions: Seq[Transaction]): Unit = {
    import spark.implicits._
    val expected = spark.createDataset(transactions).toDF()
    expected
      .write
      .mode(SaveMode.Append)
      .format(source = "jdbc")
      .options(jdbcOptions)
      .save()
  }

  private def readAndShowTransactions(format: String, offsetField: String, triggerMs: Long, timeoutMs: Long): Unit = {

    import org.apache.spark.sql.execution.streaming.sources.JDBCStreamingSourceV1.CONFIG_OFFSET_FIELD

    val stream = spark.readStream
      .format(format)
      .options(jdbcOptions + (CONFIG_OFFSET_FIELD -> offsetField))
      .load

    val query = stream
      .writeStream
        .trigger(Trigger.ProcessingTime(triggerMs))
      .format(source = "console")
      .outputMode(OutputMode.Append())
        .start()

    query.awaitTermination(timeoutMs = timeoutMs)
    query.stop()
  }

  private def randomTransactions(startDate: String, endDate: String): Seq[Transaction] = {
    val start = LocalDate.parse(startDate)
    val end = LocalDate.parse(endDate)

    (0 to ChronoUnit.DAYS.between(start, end).toInt).map(d => randomTransaction(start.plusDays(d)))
  }

  private def randomTransaction(date: LocalDate): Transaction = {
    Transaction(UUID.randomUUID().toString, randGen.nextDouble(), Date.valueOf(date))
  }
}
