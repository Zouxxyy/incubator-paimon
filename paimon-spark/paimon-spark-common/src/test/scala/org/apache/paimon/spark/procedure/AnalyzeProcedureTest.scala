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
package org.apache.paimon.spark.procedure;

import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.types.{DataField, DataFieldStats}

import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Assertions

import java.util

/** Test cases for [[AnalyzeProcedure]]. */
class AnalyzeProcedureTest extends PaimonSparkTestBase {

  test("Paimon procedure: analyze table all cols") {
    spark.sql(
      s"""
         |CREATE TABLE T (id STRING, name STRING, i INT, l LONG, d DOUBLE, f FLOAT, b BOOLEAN, dt DATE, ts TIMESTAMP)
         |USING PAIMON
         |TBLPROPERTIES ('primary-key'='id')
         |""".stripMargin)

    spark.sql(
      s"INSERT INTO T VALUES ('1', 'a', 1, 1, 1.0, 1.0, true, to_date('2020-01-01'), to_timestamp('2020-01-01 00:00:00'))")
    spark.sql(
      s"INSERT INTO T VALUES ('2', 'aaa', 1, 1, 1.0, 1.0, true, to_date('2020-01-02'), to_timestamp('2020-01-02 00:00:00'))")
    spark.sql(
      s"INSERT INTO T VALUES ('3', 'bbbb', 2, 1, 1.0, 1.0, true, to_date('2020-01-02'), to_timestamp('2020-01-02 00:00:00'))")
    spark.sql(
      s"INSERT INTO T VALUES ('4', 'bbbbbbbb', 2, 2, 2.0, 2.0, false, to_date('2020-01-01'), to_timestamp('2020-01-01 00:00:00'))")

    spark.sql(s"CALL sys.analyze(table => 'T')")
    var fields = loadTable("T").schema().fields()
    assertStatsEqual(fields, "id", new DataFieldStats(1.0d, 1L, 1L))
    assertStatsEqual(fields, "name", new DataFieldStats(1.0d, 4L, 8L))
    assertStatsEqual(fields, "i", new DataFieldStats(0.5d, 4L, 4L))
    assertStatsEqual(fields, "l", new DataFieldStats(0.5d, 8L, 8L))
    assertStatsEqual(fields, "d", new DataFieldStats(0.5d, 8L, 8L))
    assertStatsEqual(fields, "f", new DataFieldStats(0.5d, 4L, 4L))
    assertStatsEqual(fields, "b", new DataFieldStats(0.5d, 1L, 1L))
    assertStatsEqual(fields, "dt", new DataFieldStats(0.5d, 4L, 4L))
    assertStatsEqual(fields, "ts", new DataFieldStats(0.5d, 8L, 8L))

    spark.sql(
      s"INSERT INTO T VALUES ('5', 'bbbbbbbbbbbbbbbb', 3, 3, 3.0, 3.0, false, to_date('2020-01-01'), to_timestamp('2020-01-01 00:00:00'))")

    spark.sql(s"CALL sys.analyze(table => 'T')")
    fields = loadTable("T").schema().fields()
    assertStatsEqual(fields, "id", new DataFieldStats(1.0d, 1L, 1L))
    assertStatsEqual(fields, "name", new DataFieldStats(1.0d, 7L, 16L))
    assertStatsEqual(fields, "i", new DataFieldStats(0.6d, 4L, 4L))
    assertStatsEqual(fields, "l", new DataFieldStats(0.6d, 8L, 8L))
    assertStatsEqual(fields, "d", new DataFieldStats(0.6d, 8L, 8L))
    assertStatsEqual(fields, "f", new DataFieldStats(0.6d, 4L, 4L))
    assertStatsEqual(fields, "b", new DataFieldStats(0.4d, 1L, 1L))
    assertStatsEqual(fields, "dt", new DataFieldStats(0.4d, 4L, 4L))
    assertStatsEqual(fields, "ts", new DataFieldStats(0.4d, 8L, 8L))
  }

  test("Paimon procedure: analyze unsupported col") {
    spark.sql(
      s"""
         |CREATE TABLE T (id STRING, m MAP<INT, STRING>, l ARRAY<INT>, s STRUCT<i:INT, s:STRING>)
         |USING PAIMON
         |TBLPROPERTIES ('primary-key'='id')
         |""".stripMargin)

    spark.sql(s"INSERT INTO T VALUES ('1', map(1, 'a'), array(1), struct(1, 'a'))")

    assertThatThrownBy(() => spark.sql(s"CALL sys.analyze(table => 'T', cols => 'm')"))
      .hasMessageContaining("not supported")

    assertThatThrownBy(() => spark.sql(s"CALL sys.analyze(table => 'T', cols => 'l')"))
      .hasMessageContaining("not supported")

    assertThatThrownBy(() => spark.sql(s"CALL sys.analyze(table => 'T', cols => 's')"))
      .hasMessageContaining("not supported")
  }

  def assertStatsEqual(lst: util.List[DataField], name: String, stats: DataFieldStats): Unit = {
    Assertions.assertTrue(
      lst.stream().filter(f => f.name().equals(name)).findFirst().get.stats().equals(stats))
  }

}
