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

import org.scalatest.FunSuite
import org.apache.spark.sql.execution.streaming.sources.types.OffsetSupportedTypes._
import BatchQueryMaker.OffsetInclusionType._

class TestBatchQueryMaker extends FunSuite {

  private val tableName = "transactions"
  private val offsetField = "offset_field_name"

  private def assertQuery(expected: String, start: String, end: String, dataType: OffsetDataType,
                          inclusionType: InclusionType, format: Option[String]): Unit = {
    val maker = new BatchQueryMaker(tableName, offsetField, dataType, format)
    val actual = maker.make(start, end, inclusionType)

    assert(actual == expected)
  }

  private def getExpected(start: String, end: String, format: Option[String], dataType: OffsetDataType,
                          inclusionType: InclusionType): String = {

    val operation = getInclusionOperator(inclusionType)
    val baseQuery = s"SELECT * FROM $tableName WHERE "

    val condition = dataType match {
      case NUMBER => s"$offsetField $operation $start AND $offsetField <= $end"
      case STRING => s"$offsetField $operation '$start' AND $offsetField <= '$end'"
      case DATE => s"$offsetField $operation to_date('$start','${format.get}') AND " +
        s"$offsetField <= to_date('$end','${format.get}')"
    }

    baseQuery + condition
  }

  private def getInclusionOperator(inclusionType: InclusionType): String = {
    inclusionType match {
      case INCLUSIVE => ">="
      case EXCLUSIVE => ">"
    }
  }

  test(testName = s"type $NUMBER with $INCLUSIVE comparison") {
    val startOffset = "1"
    val endOffset = "10"

    val inclusionType = INCLUSIVE
    val offsetFieldType = NUMBER

    val expected = getExpected(startOffset, endOffset, None, offsetFieldType, inclusionType)
    assertQuery(expected, startOffset, endOffset, offsetFieldType, inclusionType, None)
  }

  test(testName = s"type $STRING with $INCLUSIVE comparison") {
    val startOffset = "1"
    val endOffset = "10"

    val inclusionType = INCLUSIVE
    val offsetFieldType = STRING

    val expected = getExpected(startOffset, endOffset, None, offsetFieldType, inclusionType)
    assertQuery(expected, startOffset, endOffset, offsetFieldType, inclusionType, None)
  }

  test(testName = s"type $DATE with $INCLUSIVE comparison") {
    val startOffset = "2020-01-02"
    val endOffset = "2020-01-10"
    val format = Some("YYYY-MM-DD")

    val inclusionType = INCLUSIVE
    val offsetFieldType = DATE

    val expected = getExpected(startOffset, endOffset, format, offsetFieldType, inclusionType)
    assertQuery(expected, startOffset, endOffset, offsetFieldType, inclusionType, format)
  }

  test(testName = s"type $NUMBER with $EXCLUSIVE comparison") {
    val startOffset = "1"
    val endOffset = "10"

    val inclusionType = EXCLUSIVE
    val offsetFieldType = NUMBER

    val expected = getExpected(startOffset, endOffset, None, offsetFieldType, inclusionType)
    assertQuery(expected, startOffset, endOffset, offsetFieldType, inclusionType, None)
  }

  test(testName = s"type $STRING with $EXCLUSIVE comparison") {
    val startOffset = "1"
    val endOffset = "10"

    val inclusionType = EXCLUSIVE
    val offsetFieldType = STRING

    val expected = getExpected(startOffset, endOffset, None, offsetFieldType, inclusionType)
    assertQuery(expected, startOffset, endOffset, offsetFieldType, inclusionType, None)
  }

  test(testName = s"type $DATE with $EXCLUSIVE comparison") {
    val startOffset = "2020-01-02"
    val endOffset = "2020-01-10"
    val format = Some("YYYY-MM-DD")

    val inclusionType = EXCLUSIVE
    val offsetFieldType = DATE

    val expected = getExpected(startOffset, endOffset, format, offsetFieldType, inclusionType)
    assertQuery(expected, startOffset, endOffset, offsetFieldType, inclusionType, format)
  }
}
