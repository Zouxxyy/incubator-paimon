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

import org.apache.paimon.predicate.Predicate
import org.apache.paimon.table.Table
import org.apache.paimon.table.source.ReadBuilder

import org.apache.spark.sql.PaimonUtils.fieldReference
import org.apache.spark.sql.connector.expressions.NamedReference
import org.apache.spark.sql.connector.expressions.filter.{Predicate => SparkPredicate}
import org.apache.spark.sql.connector.read.SupportsRuntimeV2Filtering

import scala.collection.JavaConverters._

trait SupportsRuntimeV2FilteringShim extends SupportsRuntimeV2Filtering {

  def readBuilder: ReadBuilder
  def table: Table
  protected var inputPartitions: Seq[PaimonInputPartition] = _

  override def filterAttributes(): Array[NamedReference] = {
    val requiredFields = readBuilder.readType().getFieldNames.asScala
    table
      .partitionKeys()
      .asScala
      .toArray
      .filter(requiredFields.contains)
      .map(fieldReference)
  }

  override def filter(predicates: Array[SparkPredicate]): Unit = {
    val converter = SparkV2FilterConverter(table.rowType())
    val partitionKeys = table.partitionKeys().asScala
    val partitionFilter: Array[Predicate] = predicates.flatMap {
      case p if SparkV2FilterConverter.isSupportedRuntimeFilter(p, partitionKeys) =>
        converter.convert(p, ignoreFailure = true)
      case _ => None
    }
    if (partitionFilter.nonEmpty) {
      readBuilder.withFilter(partitionFilter.toList.asJava)
      // set inputPartitions null to trigger to get the new splits.
      inputPartitions = null
    }
  }
}
