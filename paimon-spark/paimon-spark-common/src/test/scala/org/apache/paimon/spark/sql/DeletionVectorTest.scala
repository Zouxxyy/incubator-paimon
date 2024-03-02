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

import org.apache.paimon.data.BinaryRow
import org.apache.paimon.deletionvectors.DeletionVectorsMaintainer
import org.apache.paimon.spark.PaimonSparkTestBase

import org.apache.spark.sql.Row
import org.junit.jupiter.api.Assertions

class DeletionVectorTest extends PaimonSparkTestBase {

  test("Paimon deleteMap: deleteMap write") {
    Seq("none", "lookup").foreach(
      changelogProducer => {
        withTable("T") {
          spark.sql(s"""
                       |CREATE TABLE T (id INT, name STRING)
                       |TBLPROPERTIES (
                       | 'primary-key' = 'id',
                       | 'changelog-producer' = '$changelogProducer',
                       | 'file.format' = 'parquet',
                       | 'deletion-vectors.enabled' = 'true',
                       | 'bucket' = '1'
                       |)
                       |""".stripMargin)
          val table = loadTable("T")

          spark.sql("INSERT INTO T VALUES (1, 'aaaaaaaaaaaaaaaaaaa'), (2, 'b'), (3, 'c')")
          spark.sql("INSERT INTO T VALUES (1, 'a_new1'), (3, 'c_new1')")
          checkAnswer(
            spark.sql(s"SELECT * from T ORDER BY id"),
            Row(1, "a_new1") :: Row(2, "b") :: Row(3, "c_new1") :: Nil)

          val dvMaintainerFactory =
            new DeletionVectorsMaintainer.Factory(table.store().newIndexFileHandler())

          val deleteMap1 = dvMaintainerFactory
            .createOrRestore(table.snapshotManager().latestSnapshotId(), BinaryRow.EMPTY_ROW, 0)
            .deletionVectors()
          // 1, 3 deleted, their row positions are 0, 2
          Assertions.assertEquals(1, deleteMap1.size())
          deleteMap1
            .entrySet()
            .forEach(
              e => {
                Assertions.assertTrue(e.getValue.isDeleted(0))
                Assertions.assertTrue(e.getValue.isDeleted(2))
              })

          spark.sql("INSERT INTO T VALUES (1, 'a_new1'), (2, 'b_new1')")
          checkAnswer(
            spark.sql(s"SELECT * from T ORDER BY id"),
            Row(1, "a_new1") :: Row(2, "b_new1") :: Row(3, "c_new1") :: Nil)

          val deleteMap2 = dvMaintainerFactory
            .createOrRestore(table.snapshotManager().latestSnapshotId(), BinaryRow.EMPTY_ROW, 0)
            .deletionVectors()
          // trigger compaction, deleteMap should be empty
          Assertions.assertTrue(deleteMap2.isEmpty)

          spark.sql("INSERT INTO T VALUES (2, 'b_new2')")
          checkAnswer(
            spark.sql(s"SELECT * from T ORDER BY id"),
            Row(1, "a_new1") :: Row(2, "b_new2") :: Row(3, "c_new1") :: Nil)

          val deleteMap3 = dvMaintainerFactory
            .createOrRestore(table.snapshotManager().latestSnapshotId(), BinaryRow.EMPTY_ROW, 0)
            .deletionVectors()
          // 2 deleted, row positions is 1
          Assertions.assertEquals(1, deleteMap3.size())
          deleteMap3
            .entrySet()
            .forEach(
              e => {
                Assertions.assertTrue(e.getValue.isDeleted(1))
              })
        }
      })
  }
}
