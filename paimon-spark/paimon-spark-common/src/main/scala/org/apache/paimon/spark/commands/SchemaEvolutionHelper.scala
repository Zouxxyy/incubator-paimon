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

package org.apache.paimon.spark.commands

import org.apache.paimon.options.Options
import org.apache.paimon.schema.SchemaMergingUtils
import org.apache.paimon.spark.{SparkConnectorOptions, SparkTable, SparkTypeUtils}
import org.apache.paimon.spark.catalyst.analysis.PaimonOutputResolver
import org.apache.paimon.spark.schema.SparkSystemColumns
import org.apache.paimon.spark.util.OptionUtils
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.types.RowType

import org.apache.spark.sql.{DataFrame, PaimonUtils, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._

/** Schema evolution flags resolved from write options and session conf. */
private[spark] case class SchemaEvolutionFlags(
    typeWidening: Boolean,
    allowExplicitCast: Boolean,
    caseSensitive: Boolean)

/**
 * Schema evolution entry points for catalog writes. The two `mergeSchema` overloads commit the
 * evolved schema during analysis/planning; the companion object holds the underlying building
 * blocks (`computeFinalSchema`, `commitSchemaEvolution`, `expectedAttrsForCatalogWrite`).
 */
private[spark] trait SchemaEvolutionHelper extends WithFileStoreTable {

  val originTable: FileStoreTable

  protected var newTable: Option[FileStoreTable] = None

  override def table: FileStoreTable = newTable.getOrElse(originTable)

  /**
   * V1 catalog write entry (`WriteIntoPaimonTable.run`). Data is pre-cast by
   * [[PaimonOutputResolver]] during analysis, so this just commits the schema and returns the input
   * unchanged.
   */
  def mergeSchema(sparkSession: SparkSession, input: DataFrame, options: Options): DataFrame = {
    if (isMergeSchemaEnabled(options)) commitEvolution(sparkSession, input.schema, options)
    input
  }

  /** V2 catalog write entry (`PaimonV2Write` constructor). Commits and returns the write schema. */
  def mergeSchema(dataSchema: StructType, options: Options): StructType = {
    if (!isMergeSchemaEnabled(options)) return dataSchema
    commitEvolution(SparkSession.active, dataSchema, options)
    val writeSchema = SparkTypeUtils.fromPaimonRowType(table.schema().logicalRowType())
    val filtered = SparkSystemColumns.filterSparkSystemColumns(dataSchema)
    if (PaimonUtils.sameType(writeSchema, filtered)) dataSchema else writeSchema
  }

  /** Commit the evolved schema for the incoming data, updating `newTable`. */
  private def commitEvolution(
      sparkSession: SparkSession,
      dataSchema: StructType,
      options: Options): Unit =
    SchemaEvolutionHelper
      .commitSchemaEvolution(table, dataSchema, sparkSession, options)
      .foreach(t => newTable = Some(t))

  private def isMergeSchemaEnabled(options: Options): Boolean =
    options.get(SparkConnectorOptions.MERGE_SCHEMA) || OptionUtils.writeMergeSchemaEnabled()

  def updateTableWithOptions(options: Map[String, String]): Unit = {
    newTable = Some(table.copy(options.asJava))
  }
}

private[spark] object SchemaEvolutionHelper {

  /** Pure computation of the post-evolution schema (no side effects). */
  def computeFinalSchema(
      table: FileStoreTable,
      dataSchema: StructType,
      flags: SchemaEvolutionFlags): Option[StructType] = {
    val dataRowType = SparkTypeUtils.toPaimonType(dataSchema).asInstanceOf[RowType]
    val current = table.schema()
    val merged =
      SchemaMergingUtils.mergeSchemas(
        current,
        dataRowType,
        flags.typeWidening,
        flags.allowExplicitCast,
        flags.caseSensitive)
    if (merged.logicalRowType() == current.logicalRowType()) {
      None
    } else {
      Some(SparkTypeUtils.fromPaimonRowType(merged.logicalRowType()))
    }
  }

  /**
   * Filter system columns, resolve flags, and commit the evolved schema to storage. Returns the new
   * table only if the schema changed. Shared by catalog writes and MERGE INTO.
   */
  def commitSchemaEvolution(
      table: FileStoreTable,
      dataSchema: StructType,
      sparkSession: SparkSession,
      options: Options = new Options()): Option[FileStoreTable] = {
    val filtered = SparkSystemColumns.filterSparkSystemColumns(dataSchema)
    val flags = readFlags(sparkSession, options)
    val dataRowType = SparkTypeUtils.toPaimonType(filtered).asInstanceOf[RowType]
    if (
      table
        .store()
        .mergeSchema(dataRowType, flags.typeWidening, flags.allowExplicitCast, flags.caseSensitive)
    ) {
      Some(table.copyWithLatestSchema())
    } else {
      None
    }
  }

  /** Convert a StructType to fresh AttributeReferences (for use as resolver expected attrs). */
  def toAttributes(schema: StructType): Seq[AttributeReference] =
    schema.map(f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)())

  /**
   * Compute the resolver's expected attributes for a catalog write. When type widening is enabled
   * with `byName` resolution, returns the post-evolution attrs so the resolver can cast incoming
   * data to the widened target types; otherwise returns `table.output` unchanged.
   */
  def expectedAttrsForCatalogWrite(
      table: DataSourceV2Relation,
      querySchema: StructType,
      options: Options,
      mergeSchemaEnabled: Boolean,
      isByName: Boolean,
      sparkSession: SparkSession): Seq[Attribute] = {
    val flags = readFlags(sparkSession, options)
    if (!isByName || !mergeSchemaEnabled || !flags.typeWidening) return table.output

    table.table.asInstanceOf[SparkTable].getTable match {
      case fst: FileStoreTable =>
        val dataSchema = SparkSystemColumns.filterSparkSystemColumns(querySchema)
        computeFinalSchema(fst, dataSchema, flags)
          .map(toAttributes)
          .getOrElse(table.output)
      case _ => table.output
    }
  }

  /** Resolve schema evolution flags from write options and session conf. */
  def readFlags(
      sparkSession: SparkSession,
      options: Options = new Options()): SchemaEvolutionFlags = {
    val typeWidening = options.get(SparkConnectorOptions.TYPE_WIDENING) || OptionUtils
      .writeMergeSchemaTypeWideningEnabled()
    val allowExplicitCast = options.get(SparkConnectorOptions.EXPLICIT_CAST) || OptionUtils
      .writeMergeSchemaExplicitCastEnabled()
    val caseSensitive = sparkSession.sessionState.conf.caseSensitiveAnalysis
    SchemaEvolutionFlags(typeWidening, allowExplicitCast, caseSensitive)
  }
}
