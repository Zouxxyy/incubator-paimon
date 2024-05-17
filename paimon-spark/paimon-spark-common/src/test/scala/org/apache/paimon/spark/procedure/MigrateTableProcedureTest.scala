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

package org.apache.paimon.spark.procedure

import org.apache.paimon.hive.migrate.SparkCommiter
import org.apache.paimon.spark.{PaimonHiveTestBase, SparkTable}
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.sink.{BatchTableCommit, CommitMessage}

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation

import java.util

class MigrateTableProcedureTest extends PaimonHiveTestBase {
//  Seq("parquet", "orc", "avro").foreach(
//    format => {
  test(s"Paimon migrate table procedure: migrate  non-partitioned table") {
    withTable("paimon_tbl") {
      // create hive table
      spark.sql(s"""
                   |CREATE TABLE paimon_tbl (id STRING, name STRING, pt STRING)
                   |USING paimon tblproperties ('file.format' = 'parquet')
                   |PARTITIONED BY (pt)
                   |""".stripMargin)

      spark.sql(s"INSERT INTO paimon_tbl VALUES ('1', 'a', 'p1'), ('2', 'b', 'p2')")

      // PaimonCommitFilesCommand
      val fileStoreTable = loadTable("paimon_tbl")

      val table = spark.read
        .format("paimon")
        .load(fileStoreTable.location().toString)
        .queryExecution
        .optimizedPlan
        .collectFirst { case relation: DataSourceV2ScanRelation => relation }
        .get
        .relation
        .table
        .asInstanceOf[SparkTable]
        .getTable
        .asInstanceOf[FileStoreTable]

      val set = new util.HashSet[String]()
      set.add("pt=p1")
      set.add("pt=p2")
      val messages = new SparkCommiter(set, fileStoreTable).generatorCommitMessage()
      try {
        val commit: BatchTableCommit = fileStoreTable.newBatchWriteBuilder.newCommit
        try commit.commit(new util.ArrayList[CommitMessage](messages))
        finally if (commit != null) commit.close()
      }

      checkAnswer(
        spark.sql(s"SELECT * FROM paimon_tbl ORDER BY id"),
        Row("1", "a", "p1") :: Row("2", "b", "p2") :: Nil)
    }
  }
//    })

  Seq("parquet", "orc", "avro").foreach(
    format => {
      test(s"Paimon migrate table procedure: migrate $format partitioned table") {
        withTable("hive_tbl") {
          // create hive table
          spark.sql(s"""
                       |CREATE TABLE hive_tbl (id STRING, name STRING, pt STRING)
                       |USING $format
                       |PARTITIONED BY (pt)
                       |""".stripMargin)

          spark.sql(s"INSERT INTO hive_tbl VALUES ('1', 'a', 'p1'), ('2', 'b', 'p2')")

          spark.sql(
            s"CALL sys.migrate_table(source_type => 'hive', table => '$hiveDbName.hive_tbl', options => 'file.format=$format')")

          checkAnswer(
            spark.sql(s"SELECT * FROM hive_tbl ORDER BY id"),
            Row("1", "a", "p1") :: Row("2", "b", "p2") :: Nil)
        }
      }
    })
}
