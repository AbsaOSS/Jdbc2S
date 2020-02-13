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

import org.apache.spark.sql.execution.streaming.sources.OffsetQueryMaker.OffsetTypes.OffsetType
import org.apache.spark.sql.types.{DataType, DateType, StructField, StructType}

private[sources] object OffsetQueryMaker {

  // currently data types can only be dates, strings and numbers
  object SupportedTypes extends Enumeration {
    type SupportedType = Value
    val DATE, STRING, NUMBER = Value
  }

  object OffsetTypes extends Enumeration {
    type OffsetType = Value
    val START, END = Value
  }
}

/**
  * Creates queries to retrieve the end offset.
  *
  * This class was created because the query will be different depending on the data type.
  *
  * For instance, dates should be converted by invoking 'to_date' from Spark SQL functions where numbers don't need
  * any special treatment.
  *
  * @param table String containing the name of the table to be queried.
  * @param fieldName String containing the name of the offset field to be queried.
  * @param schema [[StructType]] to inform the type of the field.
  * @param format Option[String] containing the expected format in case the offset field is of date type.
  */
private[sources] class OffsetQueryMaker(table: String, fieldName: String, schema: StructType,
                                        format: Option[String]) {

  import OffsetQueryMaker.SupportedTypes._
  import org.apache.spark.sql.execution.streaming.sources.OffsetQueryMaker.OffsetTypes._

  private val dataType = {
    val sqlField = schema
      .fields
      .find(_.name.toLowerCase == fieldName.toLowerCase)

    sqlField match {
      case Some(field) => getSupportedType(field)
      case _ => throw new IllegalArgumentException(s"Field not found in schema: '$fieldName'")
    }
  }

  if (dataType == DATE && format.isEmpty) {
    throw new IllegalArgumentException(s"Data type is DATE for no format was specified.")
  }

  /**
    * Converts the Spark SQL data type to one of the [[SupportedType]] options.
    *
    * This piggies-back on the inference available at [[org.apache.spark.sql.execution.datasources.jdbc.JdbcRelationProvider.createRelation()]]
    */
  private def getSupportedType(field: StructField): SupportedType = {
    field.dataType match {
      case _: DateType => DATE
      case t: DataType if t.typeName.toLowerCase.contains("string") => STRING
      case _ => NUMBER
    }
  }

  /**
    * Creates a MAX SQL operation.
    */
  private def getOffsetOperationClause(offsetType: OffsetType): String = {
    offsetType match {
      case START => s"MIN($fieldName) AS START_OFFSET"
      case END => s"MAX($fieldName) AS END_OFFSET"
    }
  }

  /**
    * Creates a '>=' SQL operation based on the parameters available in the class.
    */
  private def getFilterClause(filterValue: String, offsetType: OffsetType): String = {
    val operation = offsetType match {
      case START => "<="
      case END => ">="
    }

    dataType match {
      case DATE => s"$fieldName $operation to_date('$filterValue', '${format.get}')"
      case STRING => s"$fieldName $operation '$filterValue'"
      case NUMBER => s"$fieldName $operation $filterValue"
    }
  }


  /**
    * Creates queries to retrieve the end offset.
    *
    * @param operationType [[org.apache.spark.sql.execution.streaming.sources.OffsetQueryMaker.OffsetTypes.OffsetType]] specifying which type of operation should be executed.
    * @param fromInclusive Optional starting offset, to nail down the query.
    * @return String containing the query to be executed to find the end offset.
    * @throws IllegalArgumentException if the field is of date type but no format is provided.
    */
  def create(offsetType: OffsetType, fromInclusive: Option[String]): String = {

    val query = new StringBuilder(s"SELECT ")
    query.append(getOffsetOperationClause(offsetType))
    query.append(s" FROM $table")

    if (fromInclusive.isDefined) {
      query
        .append(" WHERE ")
        .append(getFilterClause(fromInclusive.get, offsetType))
    }

    query.toString
  }
}
