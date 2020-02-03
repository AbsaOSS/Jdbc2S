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

import org.apache.spark.sql.sources.v2.reader.streaming.Offset

case class OffsetRange(start: Option[String], end: Option[String]) {

  override def toString: String = {
    s"start = '$start', end = '$end'"
  }
}

case class OffsetField(fieldName: String, range: OffsetRange) {

  override def toString: String = {
    s"Offset field = '$fieldName', Offset range = '$range'"
  }
}

/**
  * Defines an offset for JDBC streaming source.
  *
  * The argument sequence contains the names of the fields to be used as offset queries (e.g. id, time, etc) and
  * the values to be used.
  *
  * The values are treated inclusively. For example: 'fieldName = id, range = Range(1, 5)' will generate a query like:
  *
  * ... WHERE id >= 1 AND id <= 5.
  */
case class JDBCSingleFieldOffset(fieldsOffsets: OffsetField) extends Offset {

  override def json(): String = {
    JsonOffsetMapper.toJson(offset = this)
  }

  override def toString: String = {
    fieldsOffsets.toString
  }
}