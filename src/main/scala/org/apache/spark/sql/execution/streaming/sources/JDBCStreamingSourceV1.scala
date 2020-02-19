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

package org.apache.spark.sql.execution.streaming.sources

import java.util.Properties

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.execution.streaming.sources.query.BatchQueryMaker.OffsetInclusionType.InclusionType
import org.apache.spark.sql.execution.streaming.{Offset, SerializedOffset, Source}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}
import za.co.absa.spark.jdbc.streaming.source.offsets.{JDBCSingleFieldOffset, OffsetField, OffsetRange}
import org.apache.spark.sql.execution.streaming.sources.types.OffsetSupportedTypes._
import org.apache.spark.sql.execution.streaming.sources.query.{BatchQueryMaker, OffsetQueryMaker}
import za.co.absa.spark.jdbc.streaming.source.offsets.JsonOffsetMapper
import org.apache.spark.sql.execution.streaming.sources.types.OffsetTypes._
import org.apache.spark.sql.execution.streaming.sources.query.BatchQueryMaker.OffsetInclusionType._

/**
  * Container class for a local range describing a batch start and end offsets.
  * @param startInclusion [[InclusionType]] telling if the start offset should be compared using '>' (exclusive = true)
  *                       or '>=' (exclusive = false)
  */
private case class BatchRange(start: String, end: String, startInclusion: InclusionType) {
  override def toString: String = {
    s"[Start offset = '$start', end offset = '$end', start inclusion = '$startInclusion']"
  }
}

object JDBCStreamingSourceV1 {
  val CONFIG_OFFSET_FIELD = "offset.field"
  val CONFIG_START_OFFSET = "start.offset"
  val CONFIG_OFFSET_FIELD_DATE_FORMAT = "offset.field.date.format"
}

/**
  * Extension of a Spark V1 source to support JDBC streaming queries.
  *
  * Design remarks:
  *
  * 1. The basic idea is just to wrap a "batch Dataframe" loaded from Spark batch API into a stream Dataframe.
  * 2. V1 was used since it is simpler. If needed, updating to V2 should not be too hard.
  * 3. The package name is needed because access to private methods of SparkSession and SQLContext is required.
  *
  * @param sqlContext to access Spark capabilities
  * @param providerName name of the provider that instantiated this class
  * @param parameters Map containing the parameters for this source.
  *                   Mandatory: [[org.apache.spark.sql.execution.streaming.sources.JDBCStreamingSourceV1.CONFIG_OFFSET_FIELD]]
  *                   Optional: [[org.apache.spark.sql.execution.streaming.sources.JDBCStreamingSourceV1.CONFIG_START_OFFSET]]
  *
  *                    IMPORTANT: The offset field MUST have its intended representation when .toString() is invoked on its type.
  *
  * @param metadataPath directory where metadata can be stored. Same as used by Spark to save offsets, batch ids, etc.
  * @param sourceSchema [[StructType]] containing the schema expected by Catalyst.
  * @param streamingEnabled used to instruct this source to toJDBCOffset the batch data into a stream data DataFrame or not.
  *                         It is true by default and intended to be used in tests only, so that data and offsets can
  *                         be tested without invoking a whole Spark streaming pipeline.
  */
class JDBCStreamingSourceV1(sqlContext: SQLContext,
                            val providerName: String,
                            val parameters: Map[String, String],
                            metadataPath: String,
                            sourceSchema: StructType,
                            val streamingEnabled: Boolean = true) extends Source with Logging {

  private val connectionProperties = new Properties()
  parameters.foreach {
    case(key,value) => connectionProperties.setProperty(key, value)
  }

  if (sourceSchema == null) {
    throw new IllegalArgumentException("Provided schema is null.")
  }

  private val tableName = parameters.getOrElse(JDBCOptions.JDBC_TABLE_NAME,
    throw new IllegalArgumentException(s"Table name not found. Is ${JDBCOptions.JDBC_TABLE_NAME} defined?"))


  import JDBCStreamingSourceV1._

  private val offsetField = parameters.getOrElse(CONFIG_OFFSET_FIELD,
    throw new IllegalArgumentException(s"Parameter not found: $CONFIG_OFFSET_FIELD"))

  // stores the last returned offset
  private var currentOffset: Option[JDBCSingleFieldOffset] = None

  private val offsetQueryMaker: OffsetQueryMaker = new OffsetQueryMaker(tableName, offsetField,
    findSupportedType(offsetField, schema), parameters.get(CONFIG_OFFSET_FIELD_DATE_FORMAT))

  private val batchQueryMaker = new BatchQueryMaker(tableName, offsetField,
    findSupportedType(offsetField,  schema), parameters.get(CONFIG_OFFSET_FIELD_DATE_FORMAT))

  /**
    * Retrieves the schema that will be used during the query execution.
    */
  override def schema: StructType = {
    sourceSchema
  }


  /**
    * Returns the end offset for the current micro-batch. None if no data.
    *
    * If None is returned and [[Source.stop()]] was called as part of the pipeline, the execution will end.
    *
    * This method is called before each invocation to [[JDBCStreamingSourceV1.getBatch()]].
    *
    * The offsets, regardless of their types, are treated as String, so use a field that do not lose offsetting
    * capabilities if converted to string.
    *
    * IMPORTANT: This method assumes the offsets are always increasing. Since it does not make assumptions about the
    * offset data type, when checking for advances, it uses '!=' for comparison instead of '<' since offset are treated
    * as strings.
    *
    * The logic is like this:
    *
    *
    *
    * IF END OFFSET AVAILABLE ON DATA
    *     USE IT
    *
    *     IF START OFFSET AVAILABLE ON PARAMETERS
    *        USE IT
    *     ELSE
    *        USE IT FROM DATA # if there is an end one, there must also be a start one
    * ELSE
    *     SET END OFFSET TO NONE
    *
    * @return instance of [[za.co.absa.spark.jdbc.streaming.source.offsets.JDBCSingleFieldOffset]] or none.
    */
  override def getOffset: Option[Offset] = {
    if (currentOffset.isEmpty) {
      logInfo(msg = "No offset present, calculating it from the data.")
      // the resolved offset will be stored into 'currentOffset'
      resolveFirstOffset()
      logInfo(msg = s"Offsets retrieved from data: $currentOffset")
      currentOffset
    } else {
      nextEndOffset() match {
          // if the an offset was found and it changed, update the current one and return it, otherwise return empty
        case Some(candidateNewEndOffset) if isDifferentFromPreviousEndOffset(candidateNewEndOffset) =>
          updateCurrentOffsets(newEndOffset = candidateNewEndOffset)
          logInfo(msg = s"Next offset found: $currentOffset")
          currentOffset
        case _ => {
          logDebug(msg = s"No new offset found. Previous offset: $currentOffset")
          None
        }
      }
    }
  }

  /**
    * Resolves the start and end offsets.
    *
    * The logic is this:
    *
    * IF PARAMETERS(START) IS DEFINED
    *    INITIAL = PARAMETERS(START)
    * ELSE
    *    INITIAL = FIND_FIRST(FIELD)
    *
    * IF PARAMETERS(END) IS DEFINED
    *    FINAL = PARAMETERS(END)
    * ELSE
    *    FINAL = FIND_END(FIELD)
    */
  private def resolveFirstOffset(): Unit= {

    val startOffset = parameters.get(CONFIG_START_OFFSET)

    // try to find an end offset maybe from a starting point
    currentOffset = findOffset(END_OFFSET, startOffset) match {
        // no end offset was found, so there is nothing to do
      case None =>
        logInfo(msg = s"Not offsets found for field '$offsetField'")
        None
        // an end offset was found so,
      case Some(endOffset) =>
        // if there was also a start offset defined in the parameters, we're done
        val startOffset = parameters.get(CONFIG_START_OFFSET) match {
          case Some(offset) => Some(offset)
            // otherwise, let's find it from the found last offset (not a big effect in passing it here, though)
          case None => findOffset(START_OFFSET, Some(endOffset))
        }

        // sanity check
        if (startOffset.isEmpty) {
          throw new InternalError(s"End offset was found ('$endOffset') but start offset is empty.")
        }

        val offsetRange = OffsetRange(startOffset, Some(endOffset))

        logInfo(msg = s"First offsets resolved: $offsetRange")
        Some(JDBCSingleFieldOffset(OffsetField(offsetField, offsetRange)))
    }
  }

  /**
    * Finds a new end offset if it exists
    */
  private def nextEndOffset(): Option[String] = {
    findOffset(END_OFFSET, previousEndOffsetValue)
  }

  /**
    * Gets the value of the previously returned offset.
    */
  private def previousEndOffsetValue: Option[String] = {
    currentOffset
      .get
      .fieldsOffsets
      .range
      .end
  }

  /**
    * Tells if the value of the current offset has changed.
    */
  private def isDifferentFromPreviousEndOffset(endOffset: String): Boolean = {
    endOffset != currentOffset.get.fieldsOffsets.range.end.get
  }

  /**
    * Shifts the values of the current offset by making the last end the new start and setting the new end.
    * @param newEndOffset value to be used as the new offset end.
    */
  private def updateCurrentOffsets(newEndOffset: String): Unit = {
    val newStartOffset = currentOffset.get.fieldsOffsets.range.end
    val newOffsetRange = OffsetRange(newStartOffset, Some(newEndOffset))

    logInfo(msg = s"Updating offsets: FROM ${currentOffset.get.fieldsOffsets.range} TO $newOffsetRange")

    currentOffset = Some(JDBCSingleFieldOffset(OffsetField(offsetField, newOffsetRange)))
  }

  /**
    * Finds an offset, start or end.
    *
    * @param offsetType [[OffsetType]] reference.
    * @param filter Option[String] with optional filters to be used.
    *
    * @return Option[String] containing the found offset or empty.
    */
  private def findOffset(offsetType: OffsetType, filter: Option[String]): Option[String] = {
    val offsetRetrievalQuery = offsetQueryMaker.make(offsetType, filter)

    val offsetValue = getFirstColValAsString(offsetRetrievalQuery)
    logInfo(msg = s"Inferred from data as '$offsetType': $offsetValue")
    offsetValue
  }

  /**
    * Executes the query and returns the value in the first column and cell as a string.
    */
  private def getFirstColValAsString(query: String): Option[String] = {
    logDebug(s"Running offset retrieval query: { $query }")

    val firstRow = sqlContext
        .read
        .format(source = "jdbc")
        .options(parameters)
        .option(JDBCOptions.JDBC_TABLE_NAME, s"($query)")
        .load()
        .first()

    if (!firstRow.isNullAt(0)) {
      Some(firstRow.get(0).toString)
    } else {
      None
    }
  }

  /**
    * Gets the data to be returned by the current micro-batch.
    *
    * The logic is this:
    *
    * FINAL = PROVIDED_END.END
    *
    * IF PROVIDED_START IS EMPTY
    *    INITIAL = PROVIDED_END.START
    * ELSE
    *    INITIAL = PROVIDED_START.END
    *
    *
    *
    *
    * IMPORTANT 1: Since this is data source V1, the streaming engine always invokes [[getBatch()]] before invoking
    * [[getOffset]] after restarting a checkpointed query. The 'end' offset will receive the same offset pair that was
    * previously processed, which will generate duplicates. Thus, [[getBatch()]] will always return an empty DataFrame
    * if the type of the end offset is [[SerializedOffset]], since, if [[getOffset]] had been invoked, the type would be
    * [[JDBCSingleFieldOffset]].
    *
    * Also, this invocation has no effect.
    * Refer to [[org.apache.spark.sql.execution.streaming.MicroBatchExecution#populateStartOffsets]] for clarity.
    *
    *
    *
    *
    *
    * IMPORTANT 2: The query is inclusive for the start offset in the first batch, but exclusive afterwards.
    *            This is the case because in the first batch, the whole range is included, but in the next ones,
    *            the end offset of the previous batch becomes the new start, however, is has already been processed,
    *            thus, it would become a duplicate.
    *
    * More specifically, it will be inclusive every time the first 'start' argument is empty and exclusive whenever it
    * is not.
    *
    * E.g. 'getBatch(None, Offset) will result in
    *
    * SELECT fields FROM TABLE WHERE offsetField >= start_offset AND offsetField <= end_offset.
    *
    * and 'getBatch(Offset, Offset) will result in
    *
    * SELECT fields FROM TABLE WHERE offsetField > start_offset AND offsetField <= end_offset.
    *
    *
    *
    *
    * @param start Either, the offset returned by the last call to [[JDBCStreamingSourceV1.getOffset]] if successfully
    *              executed, or the latest offset related to the latest committed batch, if the job is being restarted,
    *              or None if it is the first micro-batch.
    * @param end   The offset returned by [[JDBCStreamingSourceV1.getOffset]]
    * @return A DataFrame containing the batch data with the field Dataframe.isStreaming set to true.
    */
  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {

    val batchRange = if (isFromCheckpoint(end)) {

      updateCurrentOffsetFromCheckpoint(end)

      logInfo(msg = s"Invoked with offset from checkpoint:'$currentOffset'")

      logInfo(msg = s"Looking for offsets greater than '${currentOffset.get.fieldsOffsets.range.end.get}'")
      // this call will update the 'currentOffset' if a new end offset is found, otherwise it will still be the one
      // from the last successful batch
      getOffset match {
        case Some(newEndOffset) => resolveBatchRange(start = Some(end), end = newEndOffset)
        case None => resolveBatchRange(start = Some(end), end = end) // will result in an empty result
      }
    }
    else {
      resolveBatchRange(start, end)
    }

    val batchData = getBatchData(batchRange)

    if (streamingEnabled) {
      toStreamingDataFrame(batchData)
    } else {
      batchData
    }
  }

  /**
    * Checks if the offset is coming from the checkpoint location by checking it is an instance of
    * [[org.apache.spark.sql.execution.streaming.SerializedOffset]]
    */
  private def isFromCheckpoint(offset: Offset): Boolean = {
    offset.isInstanceOf[SerializedOffset]
  }

  /**
    * Converts a [[SerializedOffset]] to a [[JDBCSingleFieldOffset]].
    */
  private def toJDBCOffset(offset: SerializedOffset): JDBCSingleFieldOffset = {
    JsonOffsetMapper.fromJson(offset.json)
  }

  /**
    * Sets [[currentOffset]] as [[JDBCSingleFieldOffset]] resulting from the JSON content of [[SerializedOffset]].
    */
  private def updateCurrentOffsetFromCheckpoint(offset: Offset): Unit = {
    currentOffset = Some(toJDBCOffset(offset.asInstanceOf[SerializedOffset]))
  }

  /**
    * Resolves the range for the next batch.
    */
  private def resolveBatchRange(start: Option[Offset], end: Offset): BatchRange = {
    if (start.isEmpty) {
      // if no start offset, the end offset range defines the whole range

      // this is not an exclusive start since the provided start offset is empty, thus
      // this is probably the first time the query is executed
      toBatchRange(end, INCLUSIVE)
    } else {
      val previousBatchRange = toBatchRange(start.get, EXCLUSIVE)
      val nextBatchRange = toBatchRange(end, EXCLUSIVE)

      // if the start offset was received, it means it belonged to the previous batch,
      // thus its end offset defines the start offset of the next batch

      // this is probably the case when the query is being restarted, and the start offset is coming from the
      // checkpoint directory

      // exclusive start is true since the start offset is the end of the previous one, which is expected
      // to have already been processed
      BatchRange(previousBatchRange.end, nextBatchRange.end, EXCLUSIVE)
    }
  }

  /**
    * Converts an [[Offset]] implementation to [[BatchRange]].
    * Required because it may be [[JDBCSingleFieldOffset]], [[SerializedOffset]] or something else.
    */
  private def toBatchRange(offset: Offset, startInclusionType: InclusionType): BatchRange = {
    offset match {
      case o: JDBCSingleFieldOffset => toBatchRange(o, startInclusionType)
      case o: SerializedOffset => toBatchRange(toJDBCOffset(o), startInclusionType)
      case o => throw new IllegalArgumentException(s"Unknown offset type: '${o.getClass.getCanonicalName}'")
    }
  }

  /**
    * Converts an instance of [[JDBCSingleFieldOffset]] to [[BatchRange]].
    * Throws if the range is invalid, i.e. one of the fields is not defined.
    */
  private def toBatchRange(offset: JDBCSingleFieldOffset, startInclusionType: InclusionType): BatchRange = {
    val range = offset.fieldsOffsets.range

    throwIfInvalidRange(range)

    BatchRange(range.start.get, range.end.get, startInclusionType)
  }

  @throws[IllegalArgumentException]
  private def throwIfInvalidRange(range: OffsetRange): Unit = {
    if (isInvalidRange(range)) {
      throw new IllegalArgumentException(s"Invalid range informed: $range")
    }
  }

  /**
    * Checks if a range is invalid, i.e. if it has either the start or end fields undefined.
    */
  private def isInvalidRange(range: OffsetRange): Boolean = {
    range.start.isEmpty || range.end.isEmpty
  }

  /**
    * Retrieves a batch of data from the informed offsets.
    */
  private def getBatchData(range: BatchRange): DataFrame = {

    logInfo(s"Retrieving batch data from range: $range")

    val query = batchQueryMaker.make(range.start, range.end, range.startInclusion)

    sqlContext
      .read
      .format(source = "jdbc")
      .options(parameters)
      .option(JDBCOptions.JDBC_TABLE_NAME, s"($query)")
      .load()
  }

  /**
    * Converts the DataFrame to streaming by getting its RDD and invoking 'sqlContext.internalCreateDataFrame'.
    */
  private def toStreamingDataFrame(data: DataFrame): DataFrame = {
    val rdd = data.queryExecution.toRdd
    sqlContext.internalCreateDataFrame(rdd, schema, isStreaming = true)
  }

  /**
    * Does nothing since no resources are allocated.
    */
  override def stop(): Unit = {
    logWarning(msg = "Stop has been invoked but will have no effect. There is nothing to be done here.")
  }

  /**
    * Gets the last offset offset resolved by this source.
    * Intended to be used in tests only.
    */
  def getLastOffset: Option[JDBCSingleFieldOffset] = {
    currentOffset
  }
}
