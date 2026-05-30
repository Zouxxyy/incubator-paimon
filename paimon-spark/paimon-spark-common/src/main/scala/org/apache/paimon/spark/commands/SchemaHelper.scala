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

import org.apache.paimon.options.{ConfigOption, Options}
import org.apache.paimon.schema.SchemaMergingUtils
import org.apache.paimon.spark.{SparkConnectorOptions, SparkTypeUtils}
import org.apache.paimon.spark.schema.SparkSystemColumns
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.types.RowType

import org.apache.spark.sql.{Column, DataFrame, PaimonUtils, SparkSession}
import org.apache.spark.sql.catalyst.expressions.AttributeReference
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
private[spark] trait SchemaHelper extends WithFileStoreTable {

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
    if (!isMergeSchemaEnabled(sparkSession, options)) return input
    val (tw, ae, cs) = SchemaHelper.readFlags(sparkSession, options)
    SchemaHelper
      .commitSchemaEvolution(table, dataSchema, tw, ae, cs)
      .foreach { t => newTable = Some(t) }
    val writeSchema = SparkTypeUtils.fromPaimonRowType(table.schema().logicalRowType())
    if (PaimonUtils.sameType(writeSchema, dataSchema)) input
    else {
      val resolve = sparkSession.sessionState.conf.resolver
      input.select(SchemaHelper.alignColumns(writeSchema, dataSchema, resolve): _*)
    }
  }

  /** V2 write entry point. Commits schema evolution and returns the write schema. */
  def mergeSchema(dataSchema: StructType, options: Options): StructType = {
    val filtered = SparkSystemColumns.filterSparkSystemColumns(dataSchema)
    val spark = SparkSession.active
    if (!isMergeSchemaEnabled(spark, options)) return dataSchema
    val (tw, ae, cs) = SchemaHelper.readFlags(spark, options)
    SchemaHelper
      .commitSchemaEvolution(table, filtered, tw, ae, cs)
      .foreach { t => newTable = Some(t) }
    val writeSchema = SparkTypeUtils.fromPaimonRowType(table.schema().logicalRowType())
    if (PaimonUtils.sameType(writeSchema, filtered)) dataSchema else writeSchema
  }

  private def isMergeSchemaEnabled(spark: SparkSession, options: Options): Boolean =
    "true".equalsIgnoreCase(
      options.toMap.getOrDefault(SparkConnectorOptions.MERGE_SCHEMA.key(), "false")) ||
      "true".equalsIgnoreCase(
        spark.conf.get("spark.paimon." + SparkConnectorOptions.MERGE_SCHEMA.key(), "false"))

  def updateTableWithOptions(options: Map[String, String]): Unit = {
    newTable = Some(table.copy(options.asJava))
  }
}

private[spark] object SchemaHelper {

  /** Step 1: Compute the post-evolution schema without committing (used by PaimonAnalysis). */
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

  /** Step 2: Commit the schema evolution (idempotent — no-op if schema unchanged). */
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

  /** Step 3: Align/cast DataFrame columns to the target schema by name. */
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

  /** Convert a StructType to fresh AttributeReferences (for use as resolver expected attrs). */
  def toAttributes(schema: StructType): Seq[AttributeReference] =
    schema.map(f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)())

  /** Read schema evolution flags (typeWidening, allowExplicitCast, caseSensitive). */
  def readFlags(
      sparkSession: SparkSession,
      options: Options = new Options()): (Boolean, Boolean, Boolean) = {
    // Read from per-write options first, then fallback to session conf.
    // Uses sparkSession.conf.get directly (not OptionUtils which relies on SparkSession.active
    // thread-local — unreliable in streaming execution threads).
    val optMap = options.toMap
    def flag(opt: ConfigOption[java.lang.Boolean]): Boolean =
      "true".equalsIgnoreCase(optMap.getOrDefault(opt.key(), "false")) ||
        "true".equalsIgnoreCase(sparkSession.conf.get("spark.paimon." + opt.key(), "false"))
    val typeWidening = flag(SparkConnectorOptions.TYPE_WIDENING)
    val allowExplicitCast = flag(SparkConnectorOptions.EXPLICIT_CAST)
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
