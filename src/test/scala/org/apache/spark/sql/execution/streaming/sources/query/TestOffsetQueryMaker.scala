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

import org.apache.spark.sql.execution.streaming.sources.types.OffsetSupportedTypes.{DATE, NUMBER, STRING}
import org.scalatest.FunSpec
import org.apache.spark.sql.execution.streaming.sources.types.OffsetTypes._

class TestOffsetQueryMaker extends FunSpec {

  private val tableName = "transactions"

  describe(description = "constructor") {

    it("should throw if field is DATE type but no format is provided") {
      val fieldName = "d"

      assertThrows[IllegalArgumentException](new OffsetQueryMaker(tableName, fieldName, DATE, None))
    }
  }

  describe(description = "END offset queries") {
    it("should use 'from' as STRING if specified with starting offset") {
      val fieldName = "e"
      val from = "random string"

      val expected = s"SELECT MAX($fieldName) AS END_OFFSET FROM $tableName WHERE $fieldName >= '$from'"

      val actual = new OffsetQueryMaker(tableName, fieldName, STRING, None).make(END_OFFSET, Some(from))

      assert(actual == expected)
    }

    it("should use 'from' as NUMBER if specified with starting offset") {
      val fieldName = "a"
      val from = "8"

      val expected = s"SELECT MAX($fieldName) AS END_OFFSET FROM $tableName WHERE $fieldName >= $from"

      val actual = new OffsetQueryMaker(tableName, fieldName, NUMBER, None).make(END_OFFSET, Some(from))

      assert(actual == expected)
    }

    it("should use 'from' as DATE if specified with starting offset") {
      val fieldName = "d"
      val format = "YYYY-MM-DD"
      val from = "2020-01-01"

      val expected = s"SELECT MAX($fieldName) AS END_OFFSET FROM $tableName WHERE $fieldName >= to_date('$from', '$format')"

      val actual = new OffsetQueryMaker(tableName, fieldName, DATE, Some(format)).make(END_OFFSET, Some(from))

      assert(expected == actual)
    }

    it("should just query max STRING field if no 'from' is specified") {
      val fieldName = "e"

      val expected = s"SELECT MAX($fieldName) AS END_OFFSET FROM $tableName"

      val actual = new OffsetQueryMaker(tableName, fieldName, STRING, None).make(END_OFFSET, fromInclusive = None)

      assert(actual == expected)
    }

    it("should just query max NUMBER field if no 'from' is specified") {
      val fieldName = "a"

      val expected = s"SELECT MAX($fieldName) AS END_OFFSET FROM $tableName"

      val actual = new OffsetQueryMaker(tableName, fieldName, NUMBER, None).make(END_OFFSET, fromInclusive = None)

      assert(actual == expected)
    }

    it("should just query max DATE field if no 'from' is specified") {
      val fieldName = "d"
      val format = "YYYY-MM-DD"

      val expected = s"SELECT MAX($fieldName) AS END_OFFSET FROM $tableName"

      val actual = new OffsetQueryMaker(tableName, fieldName, DATE, Some(format)).make(END_OFFSET, fromInclusive = None)

      assert(actual == expected)
    }
  }

  describe(description = "START offset queries") {
    it("should use 'from' as STRING if specified with starting offset") {
      val fieldName = "e"
      val from = "random string"

      val expected = s"SELECT MIN($fieldName) AS START_OFFSET FROM $tableName WHERE $fieldName <= '$from'"

      val actual = new OffsetQueryMaker(tableName, fieldName, STRING, None).make(START_OFFSET, Some(from))

      assert(actual == expected)
    }

    it("should use 'from' as NUMBER if specified with starting offset") {
      val fieldName = "a"
      val from = "8"

      val expected = s"SELECT MIN($fieldName) AS START_OFFSET FROM $tableName WHERE $fieldName <= $from"

      val actual = new OffsetQueryMaker(tableName, fieldName, NUMBER, None).make(START_OFFSET, Some(from))

      assert(actual == expected)
    }

    it("should use 'from' as DATE if specified with starting offset") {
      val fieldName = "d"
      val format = "YYYY-MM-DD"
      val from = "2020-01-01"

      val expected = s"SELECT MIN($fieldName) AS START_OFFSET FROM $tableName WHERE $fieldName <= to_date('$from', '$format')"

      val actual = new OffsetQueryMaker(tableName, fieldName, DATE, Some(format)).make(START_OFFSET, Some(from))

      assert(expected == actual)
    }

    it("should just query max STRING field if no 'from' is specified") {
      val fieldName = "e"

      val expected = s"SELECT MIN($fieldName) AS START_OFFSET FROM $tableName"

      val actual = new OffsetQueryMaker(tableName, fieldName, STRING, None).make(START_OFFSET, fromInclusive = None)

      assert(actual == expected)
    }

    it("should just query max NUMBER field if no 'from' is specified") {
      val fieldName = "a"

      val expected = s"SELECT MIN($fieldName) AS START_OFFSET FROM $tableName"

      val actual = new OffsetQueryMaker(tableName, fieldName, NUMBER, None).make(START_OFFSET, fromInclusive = None)

      assert(actual == expected)
    }

    it("should just query max DATE field if no 'from' is specified") {
      val fieldName = "d"
      val format = "YYYY-MM-DD"

      val expected = s"SELECT MIN($fieldName) AS START_OFFSET FROM $tableName"

      val actual = new OffsetQueryMaker(tableName, fieldName, DATE, Some(format)).make(START_OFFSET, fromInclusive = None)

      assert(actual == expected)
    }
  }
}
