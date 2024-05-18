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

import org.apache.paimon.spark.PaimonHiveTestBase
import org.apache.paimon.spark.commands.PaimonCommitFilesCommand

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.Row

class NativeWriterTest extends PaimonHiveTestBase {

  test(s"Paimon native writer: write non-partitioned table") {
    withTable("paimon_tbl") {
      spark.sql(s"""
                   |CREATE TABLE paimon_tbl (id STRING, name STRING)
                   |USING paimon tblproperties ('file.format' = 'parquet')
                   |""".stripMargin)

      spark.sql("set spark.paimon.fallbackV1Writer=true")
      // fallback insert
      spark.sql(s"INSERT INTO paimon_tbl VALUES ('1', 'a'), ('2', 'b')")
      PaimonCommitFilesCommand(new Path(loadTable("paimon_tbl").location().toString), Set())
        .run(spark)
      checkAnswer(
        spark.sql(s"SELECT * FROM paimon_tbl ORDER BY id"),
        Row("1", "a") :: Row("2", "b") :: Nil)

      spark.sql("set spark.paimon.fallbackV1Writer=false")
      // no fallback insert
      spark.sql(s"INSERT INTO paimon_tbl VALUES ('3', 'c')")
      // no fallback delete
      spark.sql(s"DELETE FROM paimon_tbl WHERE id = '1'")
      checkAnswer(
        spark.sql(s"SELECT * FROM paimon_tbl ORDER BY id"),
        Row("2", "b") :: Row("3", "c") :: Nil)
    }
  }

  test(s"Paimon native writer: write non-partitioned table insert then delete") {
    withTable("paimon_tbl") {
      spark.sql(s"""
                   |CREATE TABLE paimon_tbl (id STRING, name STRING)
                   |USING paimon tblproperties ('file.format' = 'parquet')
                   |""".stripMargin)

      spark.sql("set spark.paimon.fallbackV1Writer=true")
      // fallback insert
      spark.sql(s"INSERT INTO paimon_tbl VALUES ('1', 'a'), ('2', 'b')")
      PaimonCommitFilesCommand(new Path(loadTable("paimon_tbl").location().toString), Set())
        .run(spark)
      checkAnswer(
        spark.sql(s"SELECT * FROM paimon_tbl ORDER BY id"),
        Row("1", "a") :: Row("2", "b") :: Nil)

      spark.sql("set spark.paimon.fallbackV1Writer=false")
      // no fallback insert
      spark.sql(s"INSERT INTO paimon_tbl VALUES ('3', 'c')")
      // no fallback delete
      spark.sql(s"DELETE FROM paimon_tbl WHERE id = '33'")
      checkAnswer(
        spark.sql(s"SELECT * FROM paimon_tbl ORDER BY id"),
        Row("1", "a") :: Row("2", "b") :: Nil)
    }
  }

  test(s"Paimon native writer: write partitioned table") {
    withTable("paimon_tbl") {
      spark.sql(s"""
                   |CREATE TABLE paimon_tbl (id STRING, name STRING, pt STRING)
                   |USING paimon tblproperties ('file.format' = 'parquet')
                   |PARTITIONED BY (pt)
                   |""".stripMargin)

      spark.sql("set spark.paimon.fallbackV1Writer=true")
      // fallback insert
      spark.sql(s"INSERT INTO paimon_tbl VALUES ('1', 'a', 'p1'), ('2', 'b', 'p2')")
      PaimonCommitFilesCommand(
        new Path(loadTable("paimon_tbl").location().toString),
        Set("pt=p1", "pt=p2")).run(spark)
      checkAnswer(
        spark.sql(s"SELECT * FROM paimon_tbl ORDER BY id"),
        Row("1", "a", "p1") :: Row("2", "b", "p2") :: Nil)

      spark.sql("set spark.paimon.fallbackV1Writer=false")
      // no fallback insert
      spark.sql(s"INSERT INTO paimon_tbl VALUES ('3', 'c', 'p1'), ('4', 'd', 'p3')")
      // no fallback delete
      spark.sql(s"DELETE FROM paimon_tbl WHERE pt = 'p2'")
      checkAnswer(
        spark.sql(s"SELECT * FROM paimon_tbl ORDER BY id"),
        Row("1", "a", "p1") :: Row("3", "c", "p1") :: Row("4", "d", "p3") :: Nil)
    }
  }

  test(s"Paimon native writer: write partitioned table with __HIVE_DEFAULT_PARTITION__") {
    withTable("paimon_tbl") {
      spark.sql(
        s"""
           |CREATE TABLE paimon_tbl (id STRING, name STRING, pt INT)
           |USING paimon tblproperties ('file.format' = 'parquet', 'partition.default-name' = '__HIVE_DEFAULT_PARTITION__')
           |PARTITIONED BY (pt)
           |""".stripMargin)

      spark.sql("set spark.paimon.fallbackV1Writer=true")
      // fallback insert
      spark.sql(s"INSERT INTO paimon_tbl VALUES ('1', 'a', 1), ('2', 'b', null)")
      PaimonCommitFilesCommand(
        new Path(loadTable("paimon_tbl").location().toString),
        Set("pt=1", "pt=__HIVE_DEFAULT_PARTITION__")).run(spark)
      checkAnswer(
        spark.sql(s"SELECT * FROM paimon_tbl ORDER BY id"),
        Row("1", "a", 1) :: Row("2", "b", null) :: Nil)

      spark.sql("set spark.paimon.fallbackV1Writer=false")
      // no fallback insert
      spark.sql(s"INSERT INTO paimon_tbl VALUES ('3', 'c', 1), ('4', 'd', 2)")
      // no fallback delete
      spark.sql(s"DELETE FROM paimon_tbl WHERE pt = 2")
      spark.sql(s"DELETE FROM paimon_tbl WHERE id = 1")
      checkAnswer(
        spark.sql(s"SELECT * FROM paimon_tbl ORDER BY id"),
        Row("2", "b", null) :: Row("3", "c", 1) :: Nil)
    }
  }
}
