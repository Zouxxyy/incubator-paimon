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

import org.apache.paimon.catalog.{Catalog, CatalogContext, CatalogFactory, Identifier}
import org.apache.paimon.hive.migrate.SparkCommiter
import org.apache.paimon.options.Options
import org.apache.paimon.spark.catalog.Catalogs
import org.apache.paimon.spark.leafnode.PaimonLeafRunnableCommand
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.sink.{BatchTableCommit, CommitMessage}

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.catalog.CatalogTable

import java.util

import scala.collection.JavaConverters._

case class PaimonCommitFilesCommand(catalogTable: CatalogTable, partitions: Set[String])
  extends PaimonLeafRunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val fileStoreTable = {
      sparkSession.sessionState.catalogManager.currentCatalog
      val currentCatalog = sparkSession.sessionState.catalogManager.currentCatalog.name()
      val options = Catalogs.catalogOptions(currentCatalog, sparkSession.sessionState.conf)
      val catalogContext =
        CatalogContext.create(Options.fromMap(options), sparkSession.sessionState.newHadoopConf())
      val catalog: Catalog = CatalogFactory.createCatalog(catalogContext)
      catalog
        .getTable(
          Identifier.create(catalogTable.identifier.database.get, catalogTable.identifier.table))
        .asInstanceOf[FileStoreTable]
    }

    val messages = new SparkCommiter(partitions.asJava, fileStoreTable).generatorCommitMessage()
    try {
      val commit: BatchTableCommit = fileStoreTable.newBatchWriteBuilder.newCommit
      try commit.commit(new util.ArrayList[CommitMessage](messages))
      finally if (commit != null) commit.close()
    }
    Nil
  }
}
