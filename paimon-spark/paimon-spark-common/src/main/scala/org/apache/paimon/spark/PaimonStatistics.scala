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

import org.apache.paimon.format.FieldStats
import org.apache.paimon.stats.{FieldStatsArraySerializer, FullTableStats, FullTableStatsCollector}
import org.apache.paimon.table.source.DataSplit
import org.apache.paimon.types.{DataField, DataType}
import org.apache.paimon.utils.OptionalUtils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Utils
import org.apache.spark.sql.connector.expressions.NamedReference
import org.apache.spark.sql.connector.read.Statistics
import org.apache.spark.sql.connector.read.colstats.ColumnStatistics

import java.util
import java.util.{Optional, OptionalLong}

case class PaimonStatistics[T <: PaimonBaseScan](scan: T) extends Statistics with Logging {

  private lazy val rowCount: Long = scan.getSplits.map(_.rowCount).sum

  private lazy val scannedTotalSize: Long = rowCount * scan.readSchema().defaultSize

  override def sizeInBytes(): OptionalLong = OptionalLong.of(scannedTotalSize)

  override def numRows(): OptionalLong = OptionalLong.of(rowCount)

  override def columnStats(): java.util.Map[NamedReference, ColumnStatistics] = {
    val start = System.currentTimeMillis()
    val requiredFields = scan.tableRowType.getFieldNames
    val statsCollector = new FullTableStatsCollector(scan.tableRowType, requiredFields)

    scan.getSplits.foreach(
      split => {
        split.asInstanceOf[DataSplit].dataFiles().forEach {
          file => statsCollector.collect(file.valueStats())
        }
      })
    val map = new java.util.HashMap[NamedReference, ColumnStatistics]()

    statsCollector
      .extract()
      .forEach(
        (field, stats) => {
          map.put(
            Utils.fieldReference(field.name()),
            PaimonColumnStats(field.`type`(), stats, rowCount)
          )
        })

    logWarning("程序运行时间：" + (System.currentTimeMillis() - start) + "ms")
    map
  }
}

case class PaimonColumnStats(
    override val min: Optional[Object],
    override val max: Optional[Object],
    override val nullCount: OptionalLong,
    override val distinctCount: OptionalLong,
    override val avgLen: OptionalLong,
    override val maxLen: OptionalLong)
  extends ColumnStatistics

object PaimonColumnStats {

  def apply(
      fieldType: DataType,
      fullTableStats: FullTableStats,
      rowCount: Long): PaimonColumnStats = {
    val globalFieldStats = fullTableStats.tableLevelFieldStats()
    val fileFieldStats = fullTableStats.fileLevelFieldStats()

    PaimonColumnStats(
      Optional.ofNullable(SparkInternalRow.fromPaimon(fileFieldStats.minValue(), fieldType)),
      Optional.ofNullable(SparkInternalRow.fromPaimon(fileFieldStats.maxValue(), fieldType)),
      OptionalUtils.of(fileFieldStats.nullCount()),
      if (globalFieldStats.distinctRatio() == null) OptionalLong.empty()
      else OptionalLong.of((globalFieldStats.distinctRatio() * rowCount).toLong),
      OptionalUtils.of(globalFieldStats.avgLen()),
      OptionalUtils.of(globalFieldStats.maxLen())
    )
  }
}
