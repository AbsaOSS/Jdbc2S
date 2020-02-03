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

import play.api.libs.json.{Json, OFormat}

object JsonOffsetMapper {

  implicit def rangeFormat: OFormat[OffsetRange] = Json.format[OffsetRange]
  implicit def fieldFormat: OFormat[OffsetField] = Json.format[OffsetField]
  implicit def offsetFormat: OFormat[JDBCSingleFieldOffset] = Json.format[JDBCSingleFieldOffset]

  def toJson(offset: JDBCSingleFieldOffset): String = {
    Json
      .toJson(offset)
      .toString()
  }

  def fromJson(json: String): JDBCSingleFieldOffset = {
    Json.parse(json).as[JDBCSingleFieldOffset]
  }
}
