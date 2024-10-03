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

class VariantTest extends PaimonSparkTestBase {

  test("Paimon Variant: read and write variant") {
    sql("CREATE TABLE T (id INT, v VARIANT) USING paimon")
    sql(
      "INSERT INTO T VALUES (1, parse_json('{\"age\":35,\"city\":\"Chicago\"}')), (2, parse_json('{\"age\":45,\"city\":\"c\"}')), (3, parse_json('{\"age\":55,\"city\":\"d\"}'))")
//    val rows = sql("explain SELECT * FROM T where variant_get(v, '$.age', 'int') = 35").collectAsList()
    sql("SELECT * FROM T where variant_get(v, '$.age', 'int') = 35").show()
  }

  test("Paimon Variant: read and write variant1") {
    sql("CREATE TABLE T (v VARIANT) USING paimon")
    sql("INSERT INTO T VALUES (parse_json('{\"age\":35,\"city\":\"Chicago\"}'))")
    sql("SELECT * FROM T where variant_get(v, '$.age', 'int') = 35").show()
  }

  test("Paimon Variant: write shredded variant") {
    sql(
      "CREATE TABLE T (v VARIANT) USING paimon TBLPROPERTIES ('shredding.v.schema' = 'age:string,city:string')")
    sql(s"""INSERT INTO T VALUES
           |(parse_json('{\"age\":\"35\",\"city\":\"Chicago\"}')),
           |(parse_json('{\"age\":\"35\"}')),
           |(parse_json('{\"city\":\"Chicago\", \"other\":\"xxx\"}')),
           |(parse_json('{\"other1\":\"yyy\"}'))
           |""".stripMargin)

    sql("SELECT variant_get(v, '$.age', 'string') FROM T")
      .show()
  }

  test("Paimon Variant: read shredded variant") {
    sql(
      "CREATE TABLE T (v VARIANT, v_shredding VARIANT) USING paimon TBLPROPERTIES ('shredding.v_shredding.schema' = 'age:string,city:string')")
    sql(
      s"""
         |INSERT INTO T VALUES
         |((parse_json('{\"age\":\"35\",\"city\":\"Chicago\"}')), (parse_json('{\"age\":\"35\",\"city\":\"Chicago\"}'))),
         |((parse_json('{\"age\":\"35\"}')), (parse_json('{\"age\":\"35\"}'))),
         |((parse_json('{\"city\":\"Chicago\", \"other\":\"xxx\"}')), (parse_json('{\"city\":\"Chicago\", \"other\":\"xxx\"}'))),
         |((parse_json('{\"other1\":\"yyy\"}')), (parse_json('{\"other1\":\"yyy\"}')))
         |""".stripMargin)

    //    sql(
    //      "SELECT variant_get(v, '$.city', 'string') FROM T where variant_get(v_shredding, '$.age', 'int') = 35")
    //      .show()

    // read full shredding
    //    sql(
    //      "SELECT variant_get(v_shredding, '$.city', 'string') FROM T where variant_get(v_shredding, '$.age', 'int') = 35")
    //      .show()

    // read full not shredding
    //    sql(
    //      "SELECT variant_get(v_shredding, '$.other', 'string') FROM T")
    //      .show()

    // read shredding + not shredding
    //    sql(
    //      "SELECT variant_get(v_shredding, '$.other', 'string') FROM T where variant_get(v_shredding, '$.city', 'string') = 'Chicago'")
    //      .show()
  }
}
