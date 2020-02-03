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

package utils

import org.apache.spark.sql.{SaveMode, SparkSession}
import reflect.runtime.universe.TypeTag

object SparkJDBCUtils {

  def jdbcDefaultConnectionParams(tableName: String): Map[String,String] = {
    Map(
      "user" -> "whatever_user",
      "password" -> "whatever_password",
      "database" -> "h2_db",
      "driver" -> "org.h2.Driver",
      "url" -> "jdbc:h2:mem:myDb;DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=false",
      "dbtable" -> tableName
    )
  }

  def writeToJDBC[T <: Product](spark: SparkSession,
                                dbConfig: Map[String,String],
                                data: Seq[T])(implicit tag: TypeTag[T]): Unit = {
    import spark.implicits._
    spark
      .createDataset(data)
      .toDF()
      .write
      .mode(SaveMode.Overwrite)
      .format(source = "jdbc")
      .options(dbConfig)
      .save()
  }
}
