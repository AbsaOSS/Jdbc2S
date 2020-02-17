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

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.types.{StructField, StructType}

import scala.reflect.runtime.universe.TypeTag

object SparkSchemaUtils {

  /**
    * Finds a Spark SQL schema for a given class.
    */
  def getSparkSchema[T <: Product]()(implicit tag: TypeTag[T]): StructType = {
    Encoders.product[T].schema
  }

  /**
    * Checks if two Spark StructTypes are the same regardless of the nullability.
    *
    * IMPORTANT: nested fields are not checked, thus, only use this method for flat schema.
    */
  def equalsIgnoreNullability(s1: StructType, s2: StructType): Boolean = {
    s1
      .fields
      .zip(s2.fields)
      .foreach {
        case (f1, f2) =>
          // checking for name and data type since nullability may differ
          if (haveDifferentNames(f1, f2) || haveDifferentDataTypes(f1, f2)) {
            return false
          }
      }
    true
  }

  private def haveDifferentNames(f1: StructField, f2: StructField): Boolean = {
    f1.name != f2.name
  }

  private def haveDifferentDataTypes(f1: StructField, f2: StructField): Boolean = {
    f1.dataType != f2.dataType
  }
}
