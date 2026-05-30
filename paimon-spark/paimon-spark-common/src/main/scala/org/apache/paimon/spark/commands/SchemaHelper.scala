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
import org.apache.paimon.spark.{SparkConnectorOptions, SparkTypeUtils}
import org.apache.paimon.spark.schema.SparkSystemColumns
import org.apache.paimon.spark.util.OptionUtils
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.types.RowType

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Column, DataFrame, PaimonUtils, SparkSession}
import org.apache.spark.sql.functions.{col, lit, struct, transform, transform_values}
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructField, StructType}

import scala.collection.JavaConverters._

/**
 * Schema evolution for write paths. Three building blocks:
 * {{{
 *   computeFinalSchema   — determine what the table schema should become (no side-effects)
 *   commitSchemaEvolution — persist the evolved schema to storage (idempotent)
 *   alignColumns          — cast/reorder data columns to match the evolved schema
 * }}}
 *
 * Ordering differs by path:
 *   - catalog write (V1/V2): compute(analysis) → cast(analysis) → commit(execution/planning)
 *   - path-write / MERGE: commit(includes compute, execution/analysis) → cast
 *
 * @see
 *   docs/type-widening-design.md "Write path flow" table
 */
private[spark] trait SchemaHelper extends WithFileStoreTable with Logging {

  val originTable: FileStoreTable

  protected var newTable: Option[FileStoreTable] = None

  override def table: FileStoreTable = newTable.getOrElse(originTable)

  /**
   * V1 write entry point (WriteIntoPaimonTable.run). Handles both:
   *   - path-write (save(location)): data has raw source types, all steps do real work.
   *   - catalog write (saveAsTable/INSERT, v2-write=false): data pre-cast by PaimonOutputResolver,
   *     commit is the only step that does real work (compute + align are idempotent).
   */
  def mergeSchema(sparkSession: SparkSession, input: DataFrame, options: Options): DataFrame = {
    val dataSchema = SparkSystemColumns.filterSparkSystemColumns(input.schema)
    commitAndGetWriteSchema(sparkSession, dataSchema, options) match {
      case Some(writeSchema) =>
        logDebug(
          s"[SchemaHelper] V1 align: ${dataSchema.simpleString} → ${writeSchema.simpleString}")
        val resolve = sparkSession.sessionState.conf.resolver
        input.select(SchemaHelper.alignColumns(writeSchema, dataSchema, resolve): _*)
      case None => input
    }
  }

  /**
   * V2 write entry point (PaimonV2Write constructor). Returns the write schema. Cast was already
   * done by PaimonOutputResolver in the analysis phase.
   */
  def mergeSchema(dataSchema: StructType, options: Options): StructType = {
    mergeSchema(SparkSession.active, dataSchema, options)
  }

  def mergeSchema(
      sparkSession: SparkSession,
      dataSchema: StructType,
      options: Options): StructType = {
    val filtered = SparkSystemColumns.filterSparkSystemColumns(dataSchema)
    commitAndGetWriteSchema(sparkSession, filtered, options).getOrElse(dataSchema)
  }

  /** Commit schema evolution and return the write schema if it changed (None = no evolution). */
  private def commitAndGetWriteSchema(
      sparkSession: SparkSession,
      dataSchema: StructType,
      options: Options): Option[StructType] = {
    val mergeSchemaEnabled =
      options.get(SparkConnectorOptions.MERGE_SCHEMA) || OptionUtils.writeMergeSchemaEnabled()
    if (!mergeSchemaEnabled) return None

    val (typeWidening, allowExplicitCast, caseSensitive) =
      SchemaHelper.readFlags(sparkSession, options)

    SchemaHelper
      .commitSchemaEvolution(table, dataSchema, typeWidening, allowExplicitCast, caseSensitive)
      .foreach { updatedTable => newTable = Some(updatedTable) }

    val writeSchema = SparkTypeUtils.fromPaimonRowType(table.schema().logicalRowType())
    if (!PaimonUtils.sameType(writeSchema, dataSchema)) Some(writeSchema) else None
  }

  def updateTableWithOptions(options: Map[String, String]): Unit = {
    newTable = Some(table.copy(options.asJava))
  }
}

private[spark] object SchemaHelper {

  // ---------------------------------------------------------------------------
  // Step 1: Compute the post-evolution schema (used by PaimonAnalysis at analysis time)
  // ---------------------------------------------------------------------------

  def computeFinalSchema(
      table: FileStoreTable,
      dataSchema: StructType,
      typeWidening: Boolean,
      allowExplicitCast: Boolean,
      caseSensitive: Boolean): Option[StructType] = {
    val dataRowType = SparkTypeUtils.toPaimonType(dataSchema).asInstanceOf[RowType]
    val current = table.schema()
    val merged =
      SchemaMergingUtils.mergeSchemas(
        current,
        dataRowType,
        typeWidening,
        allowExplicitCast,
        caseSensitive)
    if (merged.logicalRowType() == current.logicalRowType()) None
    else Some(SparkTypeUtils.fromPaimonRowType(merged.logicalRowType()))
  }

  // ---------------------------------------------------------------------------
  // Step 2: Commit the schema evolution (idempotent)
  // ---------------------------------------------------------------------------

  def commitSchemaEvolution(
      table: FileStoreTable,
      dataSchema: StructType,
      typeWidening: Boolean = false,
      allowExplicitCast: Boolean = false,
      caseSensitive: Boolean = true): Option[FileStoreTable] = {
    val dataRowType = SparkTypeUtils.toPaimonType(dataSchema).asInstanceOf[RowType]
    if (table.store().mergeSchema(dataRowType, typeWidening, allowExplicitCast, caseSensitive)) {
      Some(table.copyWithLatestSchema())
    } else {
      None
    }
  }

  // ---------------------------------------------------------------------------
  // Step 3: Align/cast data to the target schema
  // ---------------------------------------------------------------------------

  def alignColumns(
      targetSchema: StructType,
      dataSchema: StructType,
      resolve: (String, String) => Boolean): Seq[Column] = {
    targetSchema.map {
      targetField =>
        dataSchema.find(f => resolve(f.name, targetField.name)) match {
          case Some(dataField) =>
            alignColumn(col(dataField.name), dataField.dataType, targetField, resolve)
          case _ =>
            lit(null).cast(targetField.dataType).as(targetField.name)
        }
    }
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  def readFlags(sparkSession: SparkSession, options: Options): (Boolean, Boolean, Boolean) = {
    val typeWidening = options.get(SparkConnectorOptions.TYPE_WIDENING) || OptionUtils
      .writeMergeSchemaTypeWideningEnabled()
    val allowExplicitCast = options.get(SparkConnectorOptions.EXPLICIT_CAST) || OptionUtils
      .writeMergeSchemaExplicitCastEnabled()
    val caseSensitive = sparkSession.sessionState.conf.caseSensitiveAnalysis
    (typeWidening, allowExplicitCast, caseSensitive)
  }

  private def alignColumn(
      sourceCol: Column,
      sourceType: DataType,
      targetField: StructField,
      resolve: (String, String) => Boolean): Column = {
    (sourceType, targetField.dataType) match {
      case (s: StructType, t: StructType) if !PaimonUtils.sameType(s, t) =>
        alignStruct(sourceCol, s, t, resolve).as(targetField.name)
      case (ArrayType(s: StructType, _), ArrayType(t: StructType, _))
          if !PaimonUtils.sameType(s, t) =>
        transform(sourceCol, elem => alignStruct(elem, s, t, resolve)).as(targetField.name)
      case (MapType(_, sVal: StructType, _), MapType(_, tVal: StructType, _))
          if !PaimonUtils.sameType(sVal, tVal) =>
        transform_values(sourceCol, (_, v) => alignStruct(v, sVal, tVal, resolve))
          .as(targetField.name)
      case _ if !PaimonUtils.sameType(sourceType, targetField.dataType) =>
        sourceCol.cast(targetField.dataType).as(targetField.name)
      case _ =>
        sourceCol.as(targetField.name)
    }
  }

  private def alignStruct(
      sourceCol: Column,
      sourceType: StructType,
      targetType: StructType,
      resolve: (String, String) => Boolean): Column = {
    val subCols = targetType.map {
      subTargetField =>
        sourceType.find(f => resolve(f.name, subTargetField.name)) match {
          case Some(subDataField) =>
            alignColumn(
              sourceCol.getField(subDataField.name),
              subDataField.dataType,
              subTargetField,
              resolve)
          case _ =>
            lit(null).cast(subTargetField.dataType).as(subTargetField.name)
        }
    }
    struct(subCols: _*)
  }
}
