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

import java.util.{Random, UUID}

import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicLong

private case class Signal(eventNumber: Long, user: String, value: Double)

/**
  * Uses a long field as the offset tracker.
  */
object V1NumericFieldAsOffset {

  private val eventNumberGenerator = new AtomicLong(0)
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
    * Generates some random data of format [[Signal]] from start to end date.
    */
  private class RandomDataSaver(numberOfSignals: Int) extends Runnable {
    override def run(): Unit = {
      println(s"Inserting $numberOfSignals signals ...")
      writeSignals(randomSignals(numberOfSignals))
      println(s"Done")
    }
  }

  def main(args: Array[String]): Unit = {

    val format = "za.co.absa.spark.jdbc.streaming.source.providers.JDBCStreamingSourceProviderV1"

    val signals = randomSignals(howMany = 10)
    // inserts a first batch of 10 records
    writeSignals(signals)

    val scheduler = new ScheduledThreadPoolExecutor(1)
    // inserts a second batch of data after 8 seconds
    scheduler.schedule(new RandomDataSaver(numberOfSignals = 5), 8, TimeUnit.SECONDS)

    // inserts a third batch of data after 12 seconds
    scheduler.schedule(new RandomDataSaver(numberOfSignals = 15), 12, TimeUnit.SECONDS)

    // the offset field will be 'eventNumber', which is long
    readAndShowTransactions(format = format, offsetField = "eventNumber", triggerMs = 4000L, timeoutMs = 30000L)
  }

  private def writeSignals(signals: Seq[Signal]): Unit = {
    import spark.implicits._
    val expected = spark.createDataset(signals).toDF()
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

  private def randomSignals(howMany: Int): Seq[Signal] = {
    (0 until howMany).map(_ => randomSignal(eventNumberGenerator.incrementAndGet()))
  }

  private def randomSignal(eventNumber: Long): Signal = {
    Signal(eventNumber, UUID.randomUUID().toString, randGen.nextDouble())
  }
}

