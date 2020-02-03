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

Currently only supports Spark DataSourceV1 and built on top of Spark JDBC batch source.

Will be expanded to support DataSourceV2 in the future.


## Features/Caveats

The remarkable features of this data source are:

### Offset type-agnostic
Any data type can be used as offset (date, string, int, double, custom, etc).

**Caveats** - the field must be convertible to a string representation and must also be increasing, since the comparison
with between two offsets is not done using the ``< or <=`` operators but the ```!=``` one. 

The queries however are done using ```>, >=, < and <=```. They are inclusive for the last value. More specifically, 
it will be inclusive every time the first 'start' argument is empty and exclusive whenever it is not.

As an example, the code below will be executed when Spark invokes [getBatch(None,Offset)](https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/Source.scala#L61):  

```SELECT fields FROM TABLE WHERE offsetField >= start_offset AND offsetField <= end_offset```

but when the start offset is defined, i.e. [getBatch(Offset,Offset)](https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/Source.scala#L61):

```SELECT fields FROM TABLE WHERE offsetField > start_offset AND offsetField <= end_offset```

 
### Relies on Spark JDBC batch source
This source works by wrapping the RDD from a batch DataFrame inside a streaming DataFrame, thus, whatever strengths or weaknesses present in that source will also be present in this one.

**Caveats** - to simplify the implementation, this source relies on the method ```internalCreateDataFrame``` from [SQLContext](https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/SQLContext.scala#L385).
That method, however, if package private, thus, this source had to be put in the package ```org.apache.spark.sql.execution.streaming.sources```
to be able to access that method.

### Full support for checkpointing
This source supports checkpointing as any other streaming source.

**Caveats** - Spark requires offsets to have JSON representations, so that they can be stored in the Write-Ahead Log(WAL) in that format.
When the query is interrupted, the last committed offset is loaded as an instance of [SerializedOffset](https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/SerializedOffset.scala).

Also, Spark streaming engine assumes that V1 source have the ```getBatch``` method invoked once the checkpointed offset is loaded, 
as explained in [this comment](https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/MicroBatchExecution.scala#L302).

This source, however, processes all the data informed by the last offset, thus, if it processed the offsets informed at query 
restart time, there would be duplicates. Also, it uses its own offset definition, ```JDBCSingleFieldOffset```.

So, the way to connect all these pieces is to proceed like this: if the end offset provided to [getBatch](https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/Source.scala#L61)
is of type ```SerializedOffset``` and there is no previous offset store, the incoming offset is understood as coming
from the checkpoint location. In this case, it is set as the previous offset and an empty DataFrame is returned.

In the next iteration, when calling the same method, the start offset will be the [[SerializedOffset]] instance one, 
but it will have been processed already in the last batch, so in this case, the algorithm proceeds normally.


## Usage

Examples can be found in the package ```za.co.absa.spark.jdbc.streaming.source.examples```.