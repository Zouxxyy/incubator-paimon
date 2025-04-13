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

package org.apache.paimon.spark

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.distributions.Distribution
import org.apache.spark.sql.connector.expressions.SortOrder
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriter, DataWriterFactory, PhysicalWriteInfo, RequiresDistributionAndOrdering, Write, WriterCommitMessage}

class SparkV2Write extends Write with RequiresDistributionAndOrdering {

  override def toBatch: BatchWrite = {
    // 返回 batchWrite
    null
  }

  override def requiredDistribution(): Distribution = {
    // 这个会给 dataframe 套上一个 shuffle
    null
  }

  override def requiredOrdering(): Array[SortOrder] = {
    null
  }
}

// driver 端
class PaimonBatchWrite extends BatchWrite {

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
    // 创建 DataWriterFactory, 序列化给 executor
    null
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    // commit executor 提交的 commit 信息
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    // null
  }
}

// executor 端
class PaimonWriterFactory extends DataWriterFactory {

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    // 创建相应 writer
    null
  }
}

class PaimonDataWriter extends DataWriter[InternalRow] {

  override def write(record: InternalRow): Unit = ???

  override def commit(): WriterCommitMessage = ???

  override def abort(): Unit = ???

  override def close(): Unit = ???
}
