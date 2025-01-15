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

import org.apache.paimon.{stats, CoreOptions}
import org.apache.paimon.annotation.VisibleForTesting
import org.apache.paimon.predicate.Predicate
import org.apache.paimon.spark.metric.SparkMetricRegistry
import org.apache.paimon.spark.sources.PaimonMicroBatchStream
import org.apache.paimon.spark.statistics.StatisticsHelper
import org.apache.paimon.table.{BucketMode, DataTable, FileStoreTable, Table}
import org.apache.paimon.table.source.{DataSplit, InnerTableScan, Split}

import org.apache.spark.sql.connector.expressions.{Expressions, Transform}
import org.apache.spark.sql.connector.metric.{CustomMetric, CustomTaskMetric}
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.connector.read.partitioning.{KeyGroupedPartitioning, Partitioning, UnknownPartitioning}
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

import java.util.Optional

import scala.collection.JavaConverters._

case class PaimonScan(
    table: Table,
    requiredSchema: StructType,
    filters: Seq[Predicate],
    reservedFilters: Seq[Filter],
    override val pushDownLimit: Option[Int],
    bucketedScanEnabled: Boolean = true)
  extends Scan
  with SupportsReportStatistics
  with ScanHelper
  with ColumnPruningAndPushDown
  with StatisticsHelper
  with SupportsReportPartitioning
  with SupportsRuntimeV2FilteringShim
  with SupportsReportOrderingShim {

  override val coreOptions: CoreOptions = CoreOptions.fromMap(table.options())

  lazy val statistics: Optional[stats.Statistics] = table.statistics()

  private lazy val paimonMetricsRegistry: SparkMetricRegistry = SparkMetricRegistry()

  lazy val requiredStatsSchema: StructType = {
    val fieldNames =
      readTableRowType.getFields.asScala.map(_.name) ++ reservedFilters.flatMap(_.references)
    StructType(tableSchema.filter(field => fieldNames.contains(field.name)))
  }

  @VisibleForTesting
  def getOriginSplits: Array[Split] = {
    readBuilder
      .newScan()
      .asInstanceOf[InnerTableScan]
      .withMetricsRegistry(paimonMetricsRegistry)
      .plan()
      .splits()
      .asScala
      .toArray
  }

  final def lazyInputPartitions: Seq[PaimonInputPartition] = {
    if (inputPartitions == null) {
      inputPartitions = getInputPartitions(getOriginSplits)
    }
    inputPartitions
  }

  override def toBatch: Batch = {
    PaimonBatch(lazyInputPartitions, readBuilder, metadataColumns)
  }

  override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream = {
    new PaimonMicroBatchStream(table.asInstanceOf[DataTable], readBuilder, checkpointLocation)
  }

  override def estimateStatistics(): Statistics = {
    val stats = PaimonStatistics(this)
    // When using paimon stats, we need to perform additional FilterEstimation with reservedFilters on stats.
    if (stats.paimonStatsEnabled && reservedFilters.nonEmpty) {
      filterStatistics(stats, reservedFilters)
    } else {
      stats
    }
  }

  override def supportedCustomMetrics: Array[CustomMetric] = {
    val paimonMetrics: Array[CustomMetric] = table match {
      case _: FileStoreTable =>
        Array(
          PaimonNumSplitMetric(),
          PaimonSplitSizeMetric(),
          PaimonAvgSplitSizeMetric(),
          PaimonPlanningDurationMetric(),
          PaimonScannedManifestsMetric(),
          PaimonSkippedTableFilesMetric(),
          PaimonResultedTableFilesMetric()
        )
      case _ =>
        Array.empty[CustomMetric]
    }
    super.supportedCustomMetrics() ++ paimonMetrics
  }

  override def reportDriverMetrics(): Array[CustomTaskMetric] = {
    table match {
      case _: FileStoreTable =>
        paimonMetricsRegistry.buildSparkScanMetrics()
      case _ =>
        Array.empty[CustomTaskMetric]
    }
  }

  def shouldDoBucketedScan: Boolean = {
    bucketedScanEnabled && conf.v2BucketingEnabled && extractBucketTransform.isDefined
  }

  @transient
  private lazy val extractBucketTransform: Option[Transform] = {
    table match {
      case fileStoreTable: FileStoreTable =>
        val bucketSpec = fileStoreTable.bucketSpec()
        if (bucketSpec.getBucketMode != BucketMode.HASH_FIXED) {
          None
        } else if (bucketSpec.getBucketKeys.size() > 1) {
          None
        } else {
          // Spark does not support bucket with several input attributes,
          // so we only support one bucket key case.
          assert(bucketSpec.getNumBuckets > 0)
          assert(bucketSpec.getBucketKeys.size() == 1)
          val bucketKey = bucketSpec.getBucketKeys.get(0)
          if (requiredSchema.exists(f => conf.resolver(f.name, bucketKey))) {
            Some(Expressions.bucket(bucketSpec.getNumBuckets, bucketKey))
          } else {
            None
          }
        }

      case _ => None
    }
  }

  def disableBucketedScan(): PaimonScan = {
    copy(bucketedScanEnabled = false)
  }

  // Since Spark 3.3
  override def outputPartitioning: Partitioning = {
    extractBucketTransform
      .map(bucket => new KeyGroupedPartitioning(Array(bucket), lazyInputPartitions.size))
      .getOrElse(new UnknownPartitioning(0))
  }

  override def getInputPartitions(splits: Array[Split]): Seq[PaimonInputPartition] = {
    if (!shouldDoBucketedScan || splits.exists(!_.isInstanceOf[DataSplit])) {
      return super.getInputPartitions(splits)
    }

    splits
      .map(_.asInstanceOf[DataSplit])
      .groupBy(_.bucket())
      .map {
        case (bucket, groupedSplits) =>
          PaimonBucketedInputPartition(groupedSplits, bucket)
      }
      .toSeq
  }

  override def description(): String = {
    val pushedFiltersStr = if (filters.nonEmpty) {
      ", PushedFilters: [" + filters.mkString(",") + "]"
    } else {
      ""
    }
    s"PaimonScan: [${table.name}]" + pushedFiltersStr +
      pushDownLimit.map(limit => s", Limit: [$limit]").getOrElse("")
  }
}
