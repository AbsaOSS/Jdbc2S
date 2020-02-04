    Copyright 2020 ABSA Group Limited
    
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at
    
        http://www.apache.org/licenses/LICENSE-2.0
    
    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

# Jdbc2S - JDBC Streaming Source for Spark

Offset type-agnostic JDBC streaming source for Spark with checkpoint support.

Currently only supports Spark DataSourceV1.

Will be expanded to support DataSourceV2 in the future.

## Motivation

Streaming data from RDBMS is not very usual, and when it happens, it is usually through [Change Data Capture(CDC)](https://en.wikipedia.org/wiki/Change_data_capture).

However, in some cases (e.g. legacy systems), situations happen that require RDBMS to be used as the source of some data
streaming pipeline, e.g. data ingested from mainframes into databases in an hourly fashion.

Spark is a popular streaming processing engine but it only supports RDBMS sources in batch mode, through a JDBC data source.

This project brings the same capabilities available on Spark JDBC batch DataFrames to the streaming world.


## Features

### Offset type-agnostic
Offset type-agnostic means that any data type can be used as an offset (date, string, int, double, custom, etc).

#### Caveats
The field must be convertible to a string representation and must also be increasing, since the comparison
between two offsets is not done using the `<` or `<=` operators but the `!=` one.

The queries, however, are done using `>`, `>=`, `<` and `<=`. They are inclusive for the last value. More specifically, 
it will be inclusive every time the first 'start' argument is empty and exclusive whenever it is not.

As an example, the code below will be executed when Spark invokes [getBatch(None,Offset)](https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/Source.scala#L61):  

```sql
SELECT fields FROM TABLE WHERE offsetField >= start_offset AND offsetField <= end_offset
```

but when the start offset is defined, i.e. [getBatch(Offset,Offset)](https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/Source.scala#L61):

```sql
SELECT fields FROM TABLE WHERE offsetField > start_offset AND offsetField <= end_offset
```

 
### Piggybacked on Spark JDBC batch source
This source works by wrapping the RDD from a batch DataFrame inside a streaming DataFrame thus there is nothing substantially new.

#### Caveats
To simplify the implementation, this source relies on the method `internalCreateDataFrame` from [SQLContext](https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/SQLContext.scala#L385).
That method, however, is package-private, thus, this source had to be put in the package `org.apache.spark.sql.execution.streaming.sources`
to be able to access that method.

For more details, check [this section](https://github.com/AbsaOSS/Jdbc2S/blob/master/src/main/scala/org/apache/spark/sql/execution/streaming/sources/JDBCStreamingSourceV1.scala#L427).


### Full support for checkpointing
This source supports checkpointing as any other streaming source.

#### Caveats
Spark requires offsets to have JSON representations so that they can be stored in the Write-Ahead Log in that format.
When the query is restarted, the last committed offset is loaded as an instance of [SerializedOffset](https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/SerializedOffset.scala).

Also, Spark streaming engine assumes that V1 sources have the `getBatch` method invoked once the checkpointed offset is loaded, 
as explained in [this comment](https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/MicroBatchExecution.scala#L302).

This source, however, processes all the data informed by the last offset, thus, if it processed the offsets informed at query 
restart time, there would be duplicates. Also, it uses its own offset definition, [JDBCSingleFieldOffset](https://github.com/AbsaOSS/Jdbc2S/blob/master/src/main/scala/za/co/absa/spark/jdbc/streaming/source/offsets/JDBCSingleFieldOffset.scala).

So, the way to connect all these pieces is to proceed like this: if the end offset provided to [getBatch](https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/Source.scala#L61)
is of type `SerializedOffset` and there is no previous offset memoized, the incoming offset is understood as coming
from the checkpoint location. In this case, it is set as the previous offset and an empty DataFrame is returned.

In the next iteration, when calling the same method, the start offset will be the `SerializedOffset` instance previously used,
but it will have been processed already in the last batch, so in this case, the algorithm proceeds normally.

For more information, check [this documentation](https://github.com/AbsaOSS/Jdbc2S/blob/master/src/main/scala/org/apache/spark/sql/execution/streaming/sources/JDBCStreamingSourceV1.scala#L285)

## Usage

To use this source, the configurations below can be used.

### Parameters
There are two parameters for the V1 source, one mandatory and another optional.

1. **Mandatory**: `offset.field`

This parameters specifies the name of the field to be used as the offset.

```scala
// assuming this is the case class used in the dataset
case class Transaction(user: String, value: Double, date: Date)

val jdbcOptions = {
    Map(
      "user" -> "a_user",
      "password" -> "a_password",
      "database" -> "h2_db",
      "driver" -> "org.h2.Driver",
      "url" -> "jdbc:h2:mem:myDb;DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=false",
      "dbtable" -> "transactions"
    )
}

val stream = spark.readStream
    .format(format)
    .options(jdbcOptions + ("offset.field" -> "date")) // use the field 'date' as the offset field
    .load
```


2. **Optional**: `start.offset`

This parameter defines the start offset to be used when running the query. If not specified, it will be calculated from
the data.

```scala
// assuming this is the case class used in the dataset
case class Transaction(user: String, value: Double, date: Date)

val jdbcOptions = {
    Map(
      "user" -> "a_user",
      "password" -> "a_password",
      "database" -> "h2_db",
      "driver" -> "org.h2.Driver",
      "url" -> "jdbc:h2:mem:myDb;DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=false",
      "dbtable" -> "transactions"
    )
}

val stream = spark.readStream
    .format(format)
    // runs the query starting from the 10th of January until the last date there is data available
    .options(jdbcOptions + ("offset.field" -> "date") + ("offset.start" -> "2020-01-10"))
    .load
```

### Source name
You can refer to this source either, as a fully qualified provider name or by its short name.

#### Fully qualified provider name
The fully qualified for the V1 source is **za.co.absa.spark.jdbc.streaming.source.providers.JDBCStreamingSourceProviderV1**.

To use it, you can do:

```scala
    val format = "za.co.absa.spark.jdbc.streaming.source.providers.JDBCStreamingSourceProviderV1"

    val stream = spark.readStream
      .format(format)
      .options(params)
      .load
```

#### Short name
The short name for the V1 source is `jdbc-streaming-v1` as in [here](https://github.com/AbsaOSS/Jdbc2S/blob/master/src/main/scala/za/co/absa/spark/jdbc/streaming/source/providers/JDBCStreamingSourceProviderV1.scala#L47)

To use it you'll need:

1. Create the directory `META-INF/services` under `src/main/resources`.
2. Add a file named `org.apache.spark.sql.sources.DataSourceRegister`.
3. Inside that file, add `za.co.absa.spark.jdbc.streaming.source.providers.JDBCStreamingSourceProviderV1`.

After doing that, you'll be able to do:

```scala
    val stream = spark.readStream
      .format("jdbc-streaming-v1")
      .options(params)
      .load
```


#### Examples
Examples can be found in the package `za.co.absa.spark.jdbc.streaming.source.examples`.
