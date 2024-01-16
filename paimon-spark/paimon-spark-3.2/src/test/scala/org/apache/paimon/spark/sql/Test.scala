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

import org.apache.paimon.spark.PaimonSparkTestBase

class Test extends PaimonSparkTestBase {

  test("Paimon Procedure: compact aware bucket pk table") {
    Seq(1, -1).foreach(
      bucket => {
        withTable("T") {
          spark.sql(
            s"""
               |CREATE TABLE T (id INT, value STRING, pt STRING)
               |TBLPROPERTIES ('primary-key'='id, pt', 'bucket'='$bucket', 'write-only'='true')
               |PARTITIONED BY (pt)
               |""".stripMargin)

          val table = loadTable("T")

          spark.sql(s"INSERT INTO T VALUES (1, 'a', 'p1'), (2, 'b', 'p2')")
          spark.sql(s"INSERT INTO T VALUES (3, 'c', 'p1'), (4, 'd', 'p2')")

          spark.sql(s"CALL sys.compact(table => 'T', partitions => 'pt=p1')")
        }
      })
  }

}
