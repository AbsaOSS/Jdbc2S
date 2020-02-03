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

class TestClassJDBCSingleOffset extends FunSuite {

  test(testName = "convert option Some(x) to JSON") {
    val range = OffsetRange(Some("a"), Some("b"))
    val fieldOffset = OffsetField("key_1", range)

    val expected = """{"fieldsOffsets":{"fieldName":"key_1","range":{"start":"a","end":"b"}}}"""

    val offset = JDBCSingleFieldOffset(fieldOffset)

    assert(offset.json() == expected)
  }

  test(testName = "convert Some start, None end to JSON") {
    val range = OffsetRange(Some("c"), None)
    val fieldOffset = OffsetField("key_2", range)

    val expected = """{"fieldsOffsets":{"fieldName":"key_2","range":{"start":"c"}}}"""

    val offset = JDBCSingleFieldOffset(fieldOffset)

    assert(offset.json() == expected)
  }

  test(testName = "convert None start, Some end to JSON") {
    val range = OffsetRange(None, Some("d"))
    val fieldOffset = OffsetField("key_3", range)

    val expected = """{"fieldsOffsets":{"fieldName":"key_3","range":{"end":"d"}}}"""

    val offset = JDBCSingleFieldOffset(fieldOffset)

    assert(offset.json() == expected)
  }

  test(testName = "convert None start, None end to JSON") {
    val range = OffsetRange(None, None)
    val fieldOffset = OffsetField("key_4", range)

    val expected = """{"fieldsOffsets":{"fieldName":"key_4","range":{}}}"""

    val offset = JDBCSingleFieldOffset(fieldOffset)

    assert(offset.json() == expected)
  }
}
