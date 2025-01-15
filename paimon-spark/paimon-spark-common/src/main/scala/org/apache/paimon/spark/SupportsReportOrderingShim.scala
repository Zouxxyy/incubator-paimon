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

import org.apache.paimon.table.{FileStoreTable, Table}
import org.apache.paimon.table.source.DataSplit

import org.apache.spark.sql.connector.expressions.{Expressions, SortDirection, SortOrder}
import org.apache.spark.sql.connector.read.SupportsReportOrdering

import scala.collection.JavaConverters._

trait SupportsReportOrderingShim extends SupportsReportOrdering {

  def table: Table
  def shouldDoBucketedScan: Boolean
  def lazyInputPartitions: Seq[PaimonInputPartition]

  override def outputOrdering(): Array[SortOrder] = {
    if (
      !shouldDoBucketedScan || lazyInputPartitions.exists(
        !_.isInstanceOf[PaimonBucketedInputPartition])
    ) {
      return Array.empty
    }

    val primaryKeys = table match {
      case fileStoreTable: FileStoreTable => fileStoreTable.primaryKeys().asScala
      case _ => Seq.empty
    }
    if (primaryKeys.isEmpty) {
      return Array.empty
    }

    val allSplitsKeepOrdering = lazyInputPartitions.toSeq
      .map(_.asInstanceOf[PaimonBucketedInputPartition])
      .map(_.splits.asInstanceOf[Seq[DataSplit]])
      .forall {
        splits =>
          // Only support report ordering if all matches:
          // - one `Split` per InputPartition (TODO: Re-construct splits using minKey/maxKey)
          // - `Split` is not rawConvertible so that the merge read can happen
          // - `Split` only contains one data file so it always sorted even without merge read
          splits.size < 2 && splits.forall {
            split => !split.rawConvertible() || split.dataFiles().size() < 2
          }
      }
    if (!allSplitsKeepOrdering) {
      return Array.empty
    }

    // Multi-primary keys are fine:
    // `Array(a, b)` satisfies the required ordering `Array(a)`
    primaryKeys
      .map(Expressions.identity)
      .map {
        sortExpr =>
          // Primary key can not be null, the null ordering is no matter.
          Expressions.sort(sortExpr, SortDirection.ASCENDING)
      }
      .toArray
  }
}
