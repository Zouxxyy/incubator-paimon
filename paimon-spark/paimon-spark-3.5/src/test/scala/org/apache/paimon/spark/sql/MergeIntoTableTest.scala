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

import org.apache.paimon.spark.{PaimonAppendBucketedTableTest, PaimonAppendNonBucketTableTest, PaimonPrimaryKeyBucketedTableTest, PaimonPrimaryKeyNonBucketTableTest}

import org.apache.spark.sql.Row

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt

class MergeIntoPrimaryKeyBucketedTableTest
  extends MergeIntoTableTestBase
  with MergeIntoPrimaryKeyTableTest
  with MergeIntoNotMatchedBySourceTest
  with PaimonPrimaryKeyBucketedTableTest {}

class MergeIntoPrimaryKeyNonBucketTableTest
  extends MergeIntoTableTestBase
  with MergeIntoPrimaryKeyTableTest
  with MergeIntoNotMatchedBySourceTest
  with PaimonPrimaryKeyNonBucketTableTest {}

class MergeIntoAppendBucketedTableTest
  extends MergeIntoTableTestBase
  with MergeIntoAppendTableTest
  with MergeIntoNotMatchedBySourceTest
  with PaimonAppendBucketedTableTest {}

class MergeIntoAppendNonBucketedTableTest
  extends MergeIntoTableTestBase
  with MergeIntoAppendTableTest
  with MergeIntoNotMatchedBySourceTest
  with PaimonAppendNonBucketTableTest {

  test("Paimon MergeInto: concurrent merge and compact1") {
    for (dvEnabled <- Seq("true", "true")) {
      withTable("s", "t") {
        sql("CREATE TABLE s (id INT, b INT, c INT)")
        sql("INSERT INTO s VALUES (1, 1, 1)")

        sql(
          s"CREATE TABLE t (id INT, b INT, c INT) TBLPROPERTIES ('deletion-vectors.enabled' = '$dvEnabled')")
        sql("INSERT INTO t VALUES (1, 1, 1)")

        val mergeInto = Future {
          for (_ <- 1 to 10) {
            try {
              sql("""
                    |MERGE INTO t
                    |USING s
                    |ON t.id = s.id
                    |WHEN MATCHED THEN
                    |UPDATE SET t.id = s.id, t.b = s.b + t.b, t.c = s.c + t.c
                    |""".stripMargin)
            } catch {
              case a: Throwable =>
                assert(
                  a.getMessage.contains("Conflicts during commits") || a.getMessage.contains(
                    "Missing file"))
            }
            checkAnswer(sql("SELECT count(*) FROM t"), Seq(Row(1)))
          }
        }

        val compact = Future {
          for (_ <- 1 to 10) {
            try {
              sql("CALL sys.compact(table => 't', order_strategy => 'order', order_by => 'id')")
            } catch {
              case a: Throwable => assert(a.getMessage.contains("Conflicts during commits"))
            }
            checkAnswer(sql("SELECT count(*) FROM t"), Seq(Row(1)))
          }
        }

        Await.result(mergeInto, 60.seconds)
        Await.result(compact, 60.seconds)
      }
    }
  }
}
