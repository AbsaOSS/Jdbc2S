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

package za.co.absa.spark.jdbc.streaming.source.providers

import com.google.common.annotations.VisibleForTesting
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.streaming.Source
import org.apache.spark.sql.execution.streaming.sources.JDBCStreamingSourceV1
import org.apache.spark.sql.sources.v2.DataSourceV2
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSourceProvider}
import org.apache.spark.sql.types.StructType

/**
  * Entry point for Spark streaming engine to get an instance of [[JDBCStreamingSourceV1]].
  *
  * Although [[JDBCStreamingSourceV1]] implements Spark DataSourceV1, this provider is marked as DataSourceV2 for
  * forward compatibility.
  */
class JDBCStreamingSourceProviderV1 extends StreamSourceProvider with DataSourceV2 with DataSourceRegister {

  private lazy val batchJDBCDataFrame = (sqlContext: SQLContext, parameters: Map[String, String]) => {
    sqlContext
      .sparkSession
      .read
        .format("jdbc")
        .options(parameters)
      .load
  }

  /**
    * Alias name for this data source.
    */
  override def shortName(): String = "jdbc-streaming-v1"

  /**
    * Gets the schema for this source.
    *
    * This provider will create a batch DataFrame and extract the schema from it.
    *
    * IMPORTANT: The schema passed as an argument will be ignored since the actual schema
    * will be extracted from the DataFrame retrieved from the source.
    *
    * @param sqlContext SQLContext instance used to create the batch DataFrame.
    * @param schema A possible schema informed by the client. WILL TAKE NO EFFECT.
    * @param providerName MUST be the same as provided by [[JDBCStreamingSourceProviderV1.shortName()]]
    * @param parameters Parameters for connecting to the JDBC source.
    * @return
    */
  override def sourceSchema(sqlContext: SQLContext,
                            schema: Option[StructType],
                            providerName: String,
                            parameters: Map[String, String]): (String, StructType) = {

    validateProviderName(providerName)
    (shortName(), batchJDBCDataFrame(sqlContext, parameters).schema)
  }

  /**
    * Return an instance of [[JDBCStreamingSourceV1]].
    *
    * @param sqlContext SQLContext used to interact with Spark SQL and streaming engine.
    * @param metadataPath String containing the path to batch metadata, if any.
    * @param schema Schema to be used by the source as provided by [[JDBCStreamingSourceProviderV1.sourceSchema()]]
    * @param providerName Alias for this source as provided by [[JDBCStreamingSourceProviderV1.shortName()]]
    *                     MUST be the same as provided by [[JDBCStreamingSourceProviderV1.shortName()]].
    * @param parameters Parameters set through [[org.apache.spark.sql.SparkSession.Builder.config()]]
    */
  override def createSource(sqlContext: SQLContext,
                            metadataPath: String,
                            schema: Option[StructType],
                            providerName: String,
                            parameters: Map[String, String]): Source = {

    getSource(sqlContext, metadataPath, schema, providerName, parameters, streamingEnabled = true)
  }

  /**
    * Return an instance of [[JDBCStreamingSourceV1]] which will return DataFrames from
    * [[JDBCStreamingSourceV1.getBatch()]] with 'isStreaming' set to false.
    *
    * IMPORTANT: This method is intended to be used in tests only.
    *
    * @param sqlContext SQLContext used to interact with Spark SQL and streaming engine.
    * @param metadataPath String containing the path to batch metadata, if any.
    * @param schema Schema to be used by the source as provided by [[JDBCStreamingSourceProviderV1.sourceSchema()]]
    * @param providerName Alias for this source as provided by [[JDBCStreamingSourceProviderV1.shortName()]]
    *                     MUST be the same as provided by [[JDBCStreamingSourceProviderV1.shortName()]].
    * @param parameters Parameters set through [[org.apache.spark.sql.SparkSession.Builder.config()]]
    */
  @VisibleForTesting
  def createSourceWithDisabledStreaming(sqlContext: SQLContext,
                                        metadataPath: String,
                                        schema: Option[StructType],
                                        providerName: String,
                                        parameters: Map[String, String]): Source = {

    getSource(sqlContext, metadataPath, schema, providerName, parameters, streamingEnabled = false)
  }

  private def getSource(sqlContext: SQLContext,
                        metadataPath: String,
                        schema: Option[StructType],
                        providerName: String,
                        parameters: Map[String, String],
                        streamingEnabled: Boolean): Source = {

    validateProviderName(providerName)

    val caseInsensitiveParameters = CaseInsensitiveMap(parameters)

    new JDBCStreamingSourceV1(sqlContext, providerName, caseInsensitiveParameters, metadataPath,
      batchJDBCDataFrame(sqlContext, parameters), streamingEnabled)
  }

  private def validateProviderName(name: String): Unit = {
    if (name != shortName() && name != this.getClass.getCanonicalName) {
      throw new IllegalArgumentException(s"Invalid provider name. Expected '${shortName()}' but received '$name'")
    }
  }
}
