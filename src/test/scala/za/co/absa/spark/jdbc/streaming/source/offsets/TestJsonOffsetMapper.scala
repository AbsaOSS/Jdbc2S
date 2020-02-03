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

package za.co.absa.spark.jdbc.streaming.source.offsets

import org.scalatest.FunSuite

class TestJsonOffsetMapper extends FunSuite {

  private val range = OffsetRange(Some("1"), Some("b"))
  private val fieldsValues = OffsetField("field_1", range)
  private val offset = JDBCSingleFieldOffset(fieldsValues)

  private val json = """{"fieldsOffsets":{"fieldName":"field_1","range":{"start":"1","end":"b"}}}"""

  test(testName = "convert offset to JSON") {

    val actual = JsonOffsetMapper.toJson(offset)

    assert(actual == json)
  }

  test(testName = "convert JSON to offset") {

    val actual = JsonOffsetMapper.fromJson(json)

    assert(actual.fieldsOffsets.fieldName == offset.fieldsOffsets.fieldName)
    assert(actual.fieldsOffsets.range == offset.fieldsOffsets.range)
  }
}
