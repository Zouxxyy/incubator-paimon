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
 * Schema evolution helper for write paths. All paths follow:
 * {{{
 *   Step 1: computeFinalSchema  — what the table schema should become
 *   Step 2: commitSchemaEvolution — persist the new schema
 *   Step 3: alignColumns — cast/reorder data to match the committed schema
 * }}}
 *
 * @see
 *   docs/type-widening-design.md "Write path flow" table
 */
private[spark] trait SchemaHelper extends WithFileStoreTable with Logging {

  val originTable: FileStoreTable

  protected var newTable: Option[FileStoreTable] = None

  override def table: FileStoreTable = newTable.getOrElse(originTable)

  /**
   * V1 write entry point. Called from [[WriteIntoPaimonTable.run()]] for BOTH:
   *   - path-write (save(location)): data has raw source types → all three steps do real work.
   *   - catalog write (saveAsTable/INSERT, use-v2-write=false): data arrives already cast by
   *     PaimonOutputResolver (analysis phase) → steps 1+3 are idempotent no-ops, only step 2
   *     (commit) persists the schema.
   *
   * Flow: compute finalSchema → commit → align data.
   */
  def mergeSchema(sparkSession: SparkSession, input: DataFrame, options: Options): DataFrame = {
    logDebug(
      s"[SchemaHelper] mergeSchema(DataFrame) entry, table=${table.name()}, " +
        s"dataSchema=${input.schema.simpleString}")
    val dataSchema = SparkSystemColumns.filterSparkSystemColumns(input.schema)
    val mergeSchemaEnabled =
      options.get(SparkConnectorOptions.MERGE_SCHEMA) || OptionUtils.writeMergeSchemaEnabled()
    if (!mergeSchemaEnabled) {
      return input
    }

    val filteredDataSchema = SparkSystemColumns.filterSparkSystemColumns(dataSchema)
    val (typeWidening, allowExplicitCast, caseSensitive) =
      SchemaHelper.readFlags(sparkSession, options)

    // Step 1: Compute finalSchema
    val finalSchema = SchemaHelper
      .computeFinalSchema(table, filteredDataSchema, typeWidening, allowExplicitCast, caseSensitive)
      .getOrElse(return input)

    // Step 2: Commit schema evolution
    SchemaHelper
      .commitSchemaEvolution(
        table,
        filteredDataSchema,
        typeWidening,
        allowExplicitCast,
        caseSensitive)
      .foreach { updatedTable => newTable = Some(updatedTable) }

    // Step 3: Align/cast data to finalSchema
    val resolve = sparkSession.sessionState.conf.resolver
    val cols = SchemaHelper.alignColumns(finalSchema, dataSchema, resolve)
    input.select(cols: _*)
  }

  /**
   * V2 catalog write entry point (PaimonV2Write constructor, use-v2-write=true). Returns the schema
   * the writer should use.
   *
   * Flow: steps 1+2 (compute + commit). Step 3 (cast) was already done by PaimonOutputResolver
   * during the analysis phase, so data reaching the writer is already aligned to finalSchema.
   */
  def mergeSchema(dataSchema: StructType, options: Options): StructType = {
    mergeSchema(SparkSession.active, dataSchema, options)
  }

  def mergeSchema(
      sparkSession: SparkSession,
      dataSchema: StructType,
      options: Options): StructType = {
    logDebug(
      s"[SchemaHelper] mergeSchema(StructType) entry, table=${table.name()}, " +
        s"dataSchema=${dataSchema.simpleString}")
    val mergeSchemaEnabled =
      options.get(SparkConnectorOptions.MERGE_SCHEMA) || OptionUtils.writeMergeSchemaEnabled()
    if (!mergeSchemaEnabled) {
      return dataSchema
    }

    val filteredDataSchema = SparkSystemColumns.filterSparkSystemColumns(dataSchema)
    val (typeWidening, allowExplicitCast, caseSensitive) =
      SchemaHelper.readFlags(sparkSession, options)

    // Steps 1+2: compute finalSchema + commit (cast already done by PaimonOutputResolver)
    SchemaHelper
      .commitSchemaEvolution(
        table,
        filteredDataSchema,
        typeWidening,
        allowExplicitCast,
        caseSensitive)
      .foreach { updatedTable => newTable = Some(updatedTable) }

    val writeSchema = SparkTypeUtils.fromPaimonRowType(table.schema().logicalRowType())
    if (!PaimonUtils.sameType(writeSchema, filteredDataSchema)) {
      writeSchema
    } else {
      filteredDataSchema
    }
  }

  def updateTableWithOptions(options: Map[String, String]): Unit = {
    newTable = Some(table.copy(options.asJava))
  }
}

private[spark] object SchemaHelper {

  // ---------------------------------------------------------------------------
  // Step 1: Compute the post-evolution schema (mirrors Delta's analysis-time finalSchema)
  // ---------------------------------------------------------------------------

  /**
   * Compute the merged schema WITHOUT committing. Returns Some(evolvedSchema) if it differs from
   * the table's current schema, None otherwise.
   */
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
    if (merged.logicalRowType() == current.logicalRowType()) {
      None
    } else {
      Some(SparkTypeUtils.fromPaimonRowType(merged.logicalRowType()))
    }
  }

  // ---------------------------------------------------------------------------
  // Step 2: Commit the schema evolution
  // ---------------------------------------------------------------------------

  /**
   * Merge the dataSchema into the table's schema and commit. Idempotent: if the schema already
   * matches, returns None and no commit happens.
   */
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

  /**
   * Recursively align columns from dataSchema to targetSchema by name. For nested struct fields,
   * reorder and fill nulls for missing sub-fields. Leaf type mismatches are cast.
   */
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

  /** Read schema evolution flags from options + session conf. */
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
        transform(sourceCol, elem => alignStruct(elem, s, t, resolve))
          .as(targetField.name)
      case (MapType(sKey, sVal: StructType, _), MapType(tKey, tVal: StructType, _))
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
