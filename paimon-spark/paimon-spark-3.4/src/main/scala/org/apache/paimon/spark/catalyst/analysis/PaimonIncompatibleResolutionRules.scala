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

package org.apache.paimon.spark.catalyst.analysis

import org.apache.paimon.CoreOptions
import org.apache.paimon.spark.SparkTable
import org.apache.paimon.spark.catalyst.plans.logical.{PaimonTableValuedFunctions, PaimonTableValueFunction}

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoStatement, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

import scala.collection.JavaConverters.mapAsScalaMapConverter

/** These resolution rules are incompatible between different versions of spark. */
case class PaimonIncompatibleResolutionRules(session: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsDown {

    case func: PaimonTableValueFunction if func.args.forall(_.resolved) =>
      PaimonTableValuedFunctions.resolvePaimonTableValuedFunction(session, func)

    case i: InsertIntoStatement
        if SparkSession.active.sessionState.conf
          .getConfString("spark.paimon.fallbackV1Writer", "false")
          .equals("true") =>
      i.table match {
        case DataSourceV2Relation(paimonTable: SparkTable, _, _, _, _) =>
          val sparkSession = SparkSession.active
          val options = paimonTable.getTable.options()
          val coreOptions = CoreOptions.fromMap(options)
          if (
            paimonTable.getTable.primaryKeys().isEmpty && coreOptions
              .bucket()
              .equals(-1) && coreOptions.fileFormat().getFormatIdentifier.equals("parquet")
          ) {
            val fileIndex = {
              new InMemoryFileIndex(
                sparkSession,
                Seq(new Path(coreOptions.path().toString)),
                options.asScala.toMap,
                Option.empty,
                FileStatusCache.getOrCreate(sparkSession),
                Option.apply(PartitionSpec(paimonTable.partitionSchema, Seq.empty[PartitionPath])))
            }
            val relation = HadoopFsRelation(
              fileIndex,
              paimonTable.partitionSchema,
              paimonTable.schema,
              None,
              classOf[ParquetFileFormat].newInstance(),
              options.asScala.toMap)(sparkSession)
            i.copy(table = LogicalRelation(relation))
          } else {
            i
          }
        case _ => i
      }
  }
}
