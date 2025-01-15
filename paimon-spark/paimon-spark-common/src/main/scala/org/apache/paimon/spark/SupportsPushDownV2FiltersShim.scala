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

package org.apache.paimon.spark

import org.apache.paimon.predicate.{PartitionPredicateVisitor, Predicate}
import org.apache.paimon.table.Table

import org.apache.spark.sql.PaimonUtils
import org.apache.spark.sql.connector.expressions.filter.{Predicate => SparkPredicate}
import org.apache.spark.sql.connector.read.SupportsPushDownV2Filters
import org.apache.spark.sql.sources.Filter

import scala.collection.mutable

trait SupportsPushDownV2FiltersShim extends SupportsPushDownV2Filters {

  def table: Table
  protected var pushedPaimonPredicates: Array[Predicate] = Array.empty
  protected var reservedFilters: Array[Filter] = Array.empty
  protected var hasPostScanPredicates = false
  private var pushedSparkPredicates = Array.empty[SparkPredicate]

  /** Pushes down filters, and returns filters that need to be evaluated after scanning. */
  override def pushPredicates(predicates: Array[SparkPredicate]): Array[SparkPredicate] = {
    val pushable = mutable.ArrayBuffer.empty[(SparkPredicate, Predicate)]
    val postScan = mutable.ArrayBuffer.empty[SparkPredicate]
    val reserved = mutable.ArrayBuffer.empty[Filter]

    val converter = SparkV2FilterConverter(table.rowType)
    val visitor = new PartitionPredicateVisitor(table.partitionKeys())
    predicates.foreach {
      predicate =>
        converter.convert(predicate, ignoreFailure = true) match {
          case Some(paimonPredicate) =>
            pushable.append((predicate, paimonPredicate))
            if (paimonPredicate.visit(visitor)) {
              // We need to filter the stats using filter instead of predicate.
              reserved.append(PaimonUtils.filterV2ToV1(predicate).get)
            } else {
              postScan.append(predicate)
            }
          case None =>
            postScan.append(predicate)
        }
    }

    if (pushable.nonEmpty) {
      this.pushedSparkPredicates = pushable.map(_._1).toArray
      this.pushedPaimonPredicates = pushable.map(_._2).toArray
    }
    if (reserved.nonEmpty) {
      this.reservedFilters = reserved.toArray
    }
    if (postScan.nonEmpty) {
      this.hasPostScanPredicates = true
    }
    postScan.toArray
  }

  override def pushedPredicates: Array[SparkPredicate] = {
    pushedSparkPredicates
  }
}
