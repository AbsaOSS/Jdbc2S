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

package org.apache.spark.sql.execution.streaming.sources.query

import org.apache.spark.sql.execution.streaming.sources.types.OffsetSupportedTypes._

/**
  * Creates queries to retrieve the end offset.
  *
  * This class was created because the query will be different depending on the data type.
  *
  * For instance, dates should be converted by invoking 'CAST(${my_date} AS DATE)' from Spark SQL functions where numbers don't need
  * any special treatment.
  *
  * @param table     String containing the name of the table to be queried.
  * @param fieldName String containing the name of the offset field to be queried.
  * @param dataType  [[OffsetDataType]] containing the type of the offset field.
  * @param format    Option[String] containing the expected format in case the offset field is of date type.
  */
private[sources] class OffsetQueryMaker(table: String, fieldName: String, dataType: OffsetDataType,
                                        format: Option[String]) {

  import org.apache.spark.sql.execution.streaming.sources.types.OffsetTypes._

  if (dataType == DATE && format.isEmpty) {
    throw new IllegalArgumentException(s"Data type is $DATE but no format was specified.")
  }

  /**
    * Creates a MAX SQL operation.
    */
  private def getOffsetOperationClause(offsetType: OffsetType): String = {
    offsetType match {
      case START_OFFSET => s"MIN($fieldName) AS START_OFFSET"
      case END_OFFSET => s"MAX($fieldName) AS END_OFFSET"
    }
  }

  /**
    * Creates a '>=' SQL operation based on the parameters available in the class.
    */
  private def getFilterClause(filterValue: String, offsetType: OffsetType): String = {
    val operation = offsetType match {
      case START_OFFSET => "<="
      case END_OFFSET => ">="
    }

    dataType match {
      case DATE => s"$fieldName $operation CAST('$filterValue' AS DATE)"
      case STRING => s"$fieldName $operation '$filterValue'"
      case NUMBER => s"$fieldName $operation $filterValue"
    }
  }


  /**
    * Creates queries to retrieve the end offset.
    *
    * @param offsetType    [[org.apache.spark.sql.execution.streaming.sources.types.OffsetTypes.OffsetType]] specifying which type of operation should be executed.
    * @param fromInclusive Optional starting offset, to nail down the query.
    * @return String containing the query to be executed to find the end offset.
    * @throws IllegalArgumentException if the field is of date type but no format is provided.
    */
  def make(offsetType: OffsetType, fromInclusive: Option[String]): String = {

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
