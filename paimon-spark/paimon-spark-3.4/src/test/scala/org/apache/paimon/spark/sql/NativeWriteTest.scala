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

class NativeWriteTest extends PaimonHiveTestBase {

  import testImplicits._

  test(s"Paimon native write: write") {
    withTable("paimon_tbl", "parquet_tbl") {
      withSQLConf("spark.sql.sources.useV1SourceList" -> "") {
        spark.sql(s"""
                     |CREATE TABLE paimon_tbl (id INT, name STRING, pt STRING) USING PAIMON
                     |  PARTITIONED BY (pt)
                     |""".stripMargin)
        spark.sql("INSERT INTO paimon_tbl VALUES (1, 'a', 'p1')")

        spark.sql(s"""
                     |CREATE TABLE parquet_tbl (id INT, name STRING, pt STRING) USING PARQUET
                     |  PARTITIONED BY (pt)
                     |""".stripMargin)
        spark.sql("INSERT INTO parquet_tbl VALUES (1, 'a', 'p1')")

        spark.sql("CLEAR CACHE")

        checkAnswer(
          spark.sql("SELECT * FROM paimon_tbl"),
          Seq((1, "a", "p1")).toDF()
        )
      }
    }
  }
}
