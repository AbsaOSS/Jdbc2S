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
import org.apache.spark.sql.execution.streaming.sources.types.OffsetTypes._

private[sources] object BatchQueryMaker {

  object OffsetOperationType extends Enumeration {
    type OffsetOperationType = Value

    val INCLUSIVE, EXCLUSIVE = Value
  }
}

/**
  * Creates queries to get data for the next batch.
  *
  * @param tableName
  * @param offsetField
  * @param offsetFieldType
  */
private[sources] class BatchQueryMaker(tableName: String, offsetField: String, offsetFieldType: OffsetDataType, offsetFieldDateFormat: Option[String]) {

  import BatchQueryMaker.OffsetOperationType._

  if (offsetField == DATE && offsetFieldDateFormat.isEmpty) {
    throw new IllegalArgumentException(s"Field '$offsetField' type is $DATE but no date format was informed.")
  }

  /**
    * Creates a SQL comparison operation based on data, offset and comparison types.
    */
  private def getFilterClause(filterValue: String, offsetType: OffsetType, offsetOperationType: OffsetOperationType): String = {

    val operator = getOperator(offsetType, offsetOperationType)

    offsetFieldType match {
      case DATE => s"$offsetField $operator to_date('$filterValue', '${offsetFieldDateFormat.get}')"
      case STRING => s"$offsetField $operator '$filterValue'"
      case NUMBER => s"$offsetField $operator $filterValue"
    }
  }

  private def getOperator(offsetType: OffsetType, offsetOperationType: OffsetOperationType): String = {

    offsetType match {
      case START_OFFSET => offsetOperationType match {
        case INCLUSIVE => ">="
        case EXCLUSIVE => ">"
      }
      case END_OFFSET => offsetOperationType match {
        case INCLUSIVE => "<="
        case EXCLUSIVE => "<"
      }
    }
  }

  def make(startOffset: String, endOffset: String, startOffsetOperation: OffsetOperationType): String = {
    new StringBuilder(s"SELECT * FROM $tableName WHERE ")
      .append(getFilterClause(startOffset, START_OFFSET, startOffsetOperation))
      .append(" AND ")
      .append(getFilterClause(endOffset, END_OFFSET, INCLUSIVE))
      .toString()
  }
}
