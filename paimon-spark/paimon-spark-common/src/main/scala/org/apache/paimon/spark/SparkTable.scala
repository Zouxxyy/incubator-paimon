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

import org.apache.paimon.CoreOptions
import org.apache.paimon.options.Options
import org.apache.paimon.table.{DataTable, FileStoreTable, Table}

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, TableCapability, TableCatalog}
import org.apache.spark.sql.connector.expressions.{Expressions, Transform}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.v2.FileTable
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util.{Map => JMap, Set => JSet}

import scala.collection.JavaConverters._

case class SparkTable(table: Table)
  extends FileTable(
    SparkSession.active,
    new CaseInsensitiveStringMap(table.options),
    Seq(table.options().get("path")),
    Option.empty)
  with org.apache.spark.sql.connector.catalog.Table
  with SupportsRead
  with SupportsWrite
  with PaimonPartitionManagement {

  def getTable: Table = table

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    new PaimonScanBuilder(table.copy(options.asCaseSensitiveMap))
  }

  override def name: String = table.name

  override lazy val schema: StructType = SparkTypeUtils.fromPaimonRowType(table.rowType)

  override def capabilities: JSet[TableCapability] = {
    val capabilities: JSet[TableCapability] = new java.util.HashSet[TableCapability]
    capabilities.add(TableCapability.BATCH_READ)
    capabilities.add(TableCapability.V1_BATCH_WRITE)
    capabilities.add(TableCapability.OVERWRITE_BY_FILTER)
    capabilities.add(TableCapability.OVERWRITE_DYNAMIC)
    capabilities.add(TableCapability.MICRO_BATCH_READ)
    capabilities
  }

  override lazy val fileIndex: PartitioningAwareFileIndex = {
    val options = new CaseInsensitiveStringMap(table.options)
    val paths = Seq(new Path(table.options().get("path")))
    val sparkSession = SparkSession.active
    val userSpecifiedSchema = Option.empty
    val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
    val fileStatusCache = FileStatusCache.getOrCreate(sparkSession)
    val p = PartitionSpec(partitionSchema(), Seq.empty[PartitionPath])
    new InMemoryFileIndex(
      sparkSession,
      paths,
      caseSensitiveMap,
      userSpecifiedSchema,
      fileStatusCache,
      Option.apply(p))
  }

  override def inferSchema(files: Seq[FileStatus]): Option[StructType] =
    Option.apply(SparkTypeUtils.fromPaimonRowType(table.rowType))

  override def formatName: String = "paimon"

  override def fallbackFileFormat: Class[_ <: FileFormat] = classOf[ParquetFileFormat]

  override def partitioning: Array[Transform] = {
    table.partitionKeys().asScala.map(x => Expressions.identity(x)).toArray
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder =
    try new SparkWriteBuilder(table.asInstanceOf[FileStoreTable], Options.fromMap(info.options))
    catch {
      case e: Exception =>
        throw new RuntimeException("Only FileStoreTable can be written.")
    }

  override def properties: JMap[String, String] = table match {
    case table1: DataTable =>
      val properties = new java.util.HashMap[String, String](table1.coreOptions.toMap)
      if (!table.primaryKeys.isEmpty)
        properties.put(CoreOptions.PRIMARY_KEY.key, String.join(",", table.primaryKeys))
      properties.put(TableCatalog.PROP_PROVIDER, SparkSource.NAME)
      if (table.comment.isPresent) properties.put(TableCatalog.PROP_COMMENT, table.comment.get)
      properties
    case _ => new java.util.HashMap[String, String]()
  }
}
