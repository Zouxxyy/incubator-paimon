/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.spark.sources

import org.apache.paimon.options.Options
import org.apache.paimon.spark.{InsertInto, Overwrite, SparkConnectorOptions}
import org.apache.paimon.spark.commands.{SchemaHelper, WriteIntoPaimonTable}
import org.apache.paimon.table.FileStoreTable

import org.apache.spark.sql.{DataFrame, PaimonUtils, SQLContext}
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources.AlwaysTrue
import org.apache.spark.sql.streaming.OutputMode

class PaimonSink(
    sqlContext: SQLContext,
    override val originTable: FileStoreTable,
    partitionColumns: Seq[String],
    outputMode: OutputMode,
    options: Options)
  extends Sink
  with SchemaHelper {

  // Propagate schema evolution flags from per-write options to session conf. On some Spark
  // versions (e.g. 3.3), streaming per-write options may not reliably reach WriteIntoPaimonTable
  // via the Options object. Session conf is the universal fallback read by SchemaHelper.readFlags.
  locally {
    val spark = sqlContext.sparkSession
    val optMap = options.toMap
    Seq(
      SparkConnectorOptions.MERGE_SCHEMA,
      SparkConnectorOptions.TYPE_WIDENING,
      SparkConnectorOptions.EXPLICIT_CAST).foreach {
      opt =>
        val value = optMap.getOrDefault(opt.key(), null)
        if (value != null) {
          spark.conf.set("spark.paimon." + opt.key(), value)
        }
    }
  }

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    val saveMode = if (outputMode == OutputMode.Complete()) {
      Overwrite(Some(AlwaysTrue))
    } else {
      InsertInto
    }
    val newData = PaimonUtils.createNewDataFrame(data)
    WriteIntoPaimonTable(originTable, saveMode, newData, options, Some(batchId)).run(
      sqlContext.sparkSession)
  }
}
