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

import org.scalatest.FunSuite
import utils.TestClass
import utils.SparkSchemaUtils._
import OffsetSupportedTypes._

class TestOffsetSupportedTypes extends FunSuite {

  private val schema = getSparkSchema[TestClass]

  test(testName = "should throw if field name is not in schema") {
    assertThrows[IllegalArgumentException](findSupportedType(fieldName = "not_there", schema))
  }

  test(testName = s"should identify ${DATE} type") {
    assert(findSupportedType(fieldName = "d", schema) == DATE)
  }

  test(testName = s"should identify ${STRING} type") {
    assert(findSupportedType(fieldName = "e", schema) == STRING)
  }

  test(testName = s"should identify INT as ${NUMBER} type") {
    assert(findSupportedType(fieldName = "a", schema) == NUMBER)
  }

  test(testName = s"should identify LONG as ${NUMBER} type") {
    assert(findSupportedType(fieldName = "b", schema) == NUMBER)
  }

  test(testName = s"should identify DOUBLE as ${NUMBER} type") {
    assert(findSupportedType(fieldName = "c", schema) == NUMBER)
  }
}
