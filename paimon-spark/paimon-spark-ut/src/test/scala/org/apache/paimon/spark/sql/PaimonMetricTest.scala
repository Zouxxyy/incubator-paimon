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

package org.apache.paimon.spark.sql

import org.apache.paimon.spark.PaimonMetrics.{RESULTED_TABLE_FILES, SKIPPED_TABLE_FILES}
import org.apache.paimon.spark.PaimonSparkTestBase

import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.junit.jupiter.api.Assertions

class PaimonMetricTest extends PaimonSparkTestBase {

  test("Paimon Procedure: 1") {
    Seq("zorder").foreach {
      orderStrategy =>
        {
          withTable("T") {
            spark.sql(s"""
                         |CREATE TABLE T (id INT, pt STRING)
                         |PARTITIONED BY (pt)
                         |""".stripMargin)

            spark.sql(s"""INSERT INTO T VALUES
                         |(1, 'p1'), (3, 'p1'),
                         |(1, 'p2'), (4, 'p2'),
                         |(3, 'p3'), (2, 'p3'),
                         |(1, 'p4'), (2, 'p4')
                         |""".stripMargin)

            spark.sql(s"""INSERT INTO T VALUES
                         |(4, 'p1'), (2, 'p1'),
                         |(2, 'p2'), (3, 'p2'),
                         |(1, 'p3'), (4, 'p3'),
                         |(3, 'p4'), (4, 'p4')
                         |""".stripMargin)

            sql(
              s"CALL sys.compact(table => 'T', order_strategy => '$orderStrategy', order_by => 'id')")
          }
        }
    }
  }

  test("xxxx") {
    sql(
      """
        |CREATE TABLE my_table_cow2 (id INT,name STRING,added_channel string,age int,ts bigint, pt STRING)
        |using paimon
        |PARTITIONED BY (pt)
        |TBLPROPERTIES (
        |  'primary-key' = 'id,pt',
        |  'merge-engine' = 'partial-update',
        |  'full-compaction.delta-commits' = '1'
        |);
        |""".stripMargin)

    sql(
      """
        |insert into my_table_cow2(id,age,pt) VALUES(1,10,1);
        |""".stripMargin)

    sql(
      """
        |insert into my_table_cow2(id,age,pt) VALUES(1,20,1);
        |""".stripMargin)

    sql(
      """
        |insert into my_table_cow2(id,age,pt) VALUES(1,30,1);
        |""".stripMargin)

    sql(
      """
        |insert into my_table_cow2(id,age,pt) VALUES(1,40,1);
        |""".stripMargin)

    val a = 1
  }

  test(s"Paimon Metric: scan driver metric") {
    // Spark support reportDriverMetrics since Spark 3.4
    if (gteqSpark3_4) {
      sql(s"""
             |CREATE TABLE T (id INT, name STRING, pt STRING)
             |TBLPROPERTIES ('bucket'='1', 'bucket-key'='id', 'write-only'='true')
             |PARTITIONED BY (pt)
             |""".stripMargin)

      sql(s"INSERT INTO T VALUES (1, 'a', 'p1'), (2, 'b', 'p2')")
      sql(s"INSERT INTO T VALUES (3, 'c', 'p2'), (4, 'c', 'p3')")
      sql(s"INSERT INTO T VALUES (5, 'd', 'p2')")

      def checkMetrics(s: String, skippedTableFiles: Long, resultedTableFiles: Long): Unit = {
        val scan = getPaimonScan(s)
        // call getInputPartitions to trigger scan
        scan.lazyInputPartitions
        val metrics = scan.reportDriverMetrics()
        Assertions.assertEquals(skippedTableFiles, metric(metrics, SKIPPED_TABLE_FILES))
        Assertions.assertEquals(resultedTableFiles, metric(metrics, RESULTED_TABLE_FILES))
      }

      checkMetrics(s"SELECT * FROM T", 0, 5)
      checkMetrics(s"SELECT * FROM T WHERE pt = 'p2'", 2, 3)

      sql(s"DELETE FROM T WHERE pt = 'p1'")
      checkMetrics(s"SELECT * FROM T", 0, 4)

      sql("CALL sys.compact(table => 'T', partitions => 'pt=\"p2\"')")
      checkMetrics(s"SELECT * FROM T", 0, 2)
      checkMetrics(s"SELECT * FROM T WHERE pt = 'p2'", 1, 1)
    }
  }

  test("Paimon Metric: report output metric") {
    sql(s"CREATE TABLE T (id int)")

    var recordsWritten = 0L
    var bytesWritten = 0L

    val listener = new SparkListener() {
      override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
        val outputMetrics = taskEnd.taskMetrics.outputMetrics
        recordsWritten += outputMetrics.recordsWritten
        bytesWritten += outputMetrics.bytesWritten
      }
    }

    try {
      spark.sparkContext.addSparkListener(listener)
      sql(s"INSERT INTO T VALUES 1, 2, 3")
    } finally {
      spark.sparkContext.removeSparkListener(listener)
    }

    Assertions.assertEquals(3, recordsWritten)
    Assertions.assertTrue(bytesWritten > 0)
  }

  def metric(metrics: Array[CustomTaskMetric], name: String): Long = {
    metrics.find(_.name() == name).get.value()
  }
}
