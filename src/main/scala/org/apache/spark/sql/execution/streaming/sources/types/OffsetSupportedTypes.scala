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

package org.apache.spark.sql.execution.streaming.sources.types

import org.apache.spark.sql.types.{DataType, DateType, StructField, StructType}

/**
  * Currently data types can only be dates, strings and numbers
  */
object OffsetSupportedTypes extends Enumeration {
  type OffsetDataType = Value
  val DATE, STRING, NUMBER = Value

  /**
    * Finds the type of a field based on the schema.
    *
    * @param fieldName name of the field
    * @param schema [[StructType]] containing the available fields
    * @return [[OffsetDataType]] containing the field type
    * @throws IllegalArgumentException if the field name is not contained in the schema
    */
  def findSupportedType(fieldName: String, schema: StructType): OffsetDataType = {
    val sqlField = schema
      .fields
      .find(_.name.toLowerCase == fieldName.toLowerCase)

    sqlField match {
      case Some(field) => getSupportedType(field)
      case _ => throw new IllegalArgumentException(s"Field not found in schema: '$fieldName'")
    }
  }

  /**
    * Converts the Spark SQL data type to one of the [[OffsetDataType]] options.
    *
    * This piggies-back on the inference available at [[org.apache.spark.sql.execution.datasources.jdbc.JdbcRelationProvider.createRelation()]]
    */
  private def getSupportedType(field: StructField): OffsetDataType = {
    field.dataType match {
      case _: DateType => DATE
      case t: DataType if t.typeName.toLowerCase.contains("string") => STRING
      case _ => NUMBER
    }
  }
}
