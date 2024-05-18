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

package org.apache.paimon.spark.commands

import org.apache.paimon.hive.migrate.SparkCommiter
import org.apache.paimon.spark.SparkTable
import org.apache.paimon.spark.leafnode.PaimonLeafRunnableCommand
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.sink.{BatchTableCommit, CommitMessage}

import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation

import java.util

import scala.collection.JavaConverters._

case class PaimonCommitFilesCommand(path: Path, partitions: Set[String])
  extends PaimonLeafRunnableCommand
  with Logging {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val startTimeMillis = System.currentTimeMillis()
    val fileStoreTable = {
      sparkSession.read
        .format("paimon")
        .load(path.toString)
        .queryExecution
        .optimizedPlan
        .collectFirst { case relation: DataSourceV2ScanRelation => relation }
        .get
        .relation
        .table
        .asInstanceOf[SparkTable]
        .getTable
        .asInstanceOf[FileStoreTable]
    }

    val messages = new SparkCommiter(partitions.asJava, fileStoreTable).generatorCommitMessage()
    val commit = fileStoreTable.newBatchWriteBuilder.newCommit
    try {
      commit.commit(new util.ArrayList[CommitMessage](messages))
    } finally if (commit != null) commit.close()
    logWarning(
      s"Commit for ${fileStoreTable.name()} in ${System.currentTimeMillis() - startTimeMillis} ms")
    Nil
  }
}
