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

import org.apache.paimon.spark.catalyst.analysis.expressions.ExpressionHelper

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.{Assignment, LogicalPlan, MergeAction, MergeIntoTable, Project}

object PaimonMergeIntoResolver extends PaimonMergeIntoResolverBase with ExpressionHelper {

  override protected def newResolver(spark: SparkSession): MergeExpressionResolver = {
    new LegacyMergeExpressionResolver(spark)
  }

  override protected def resolveNotMatchedBySourceActions(
      merge: MergeIntoTable,
      resolve: MergeExpressionResolver): Seq[MergeAction] = {
    Seq.empty
  }

  def build(
      merge: MergeIntoTable,
      resolvedCond: Expression,
      resolvedMatched: Seq[MergeAction],
      resolvedNotMatched: Seq[MergeAction],
      resolvedNotMatchedBySource: Seq[MergeAction]): MergeIntoTable = {
    if (resolvedNotMatchedBySource.nonEmpty) {
      throw new RuntimeException("WHEN NOT MATCHED BY SOURCE is not supported here.")
    }

    merge.copy(
      mergeCondition = resolvedCond,
      matchedActions = resolvedMatched,
      notMatchedActions = resolvedNotMatched)
  }

  final private class LegacyMergeExpressionResolver(spark: SparkSession)
    extends MergeExpressionResolver {

    private val resolve: (Expression, LogicalPlan) => Expression = resolveExpression(spark)

    override def resolveCondition(
        condition: Expression,
        mergeInto: MergeIntoTable,
        resolvedWith: ResolvedWith): Expression = {
      resolvedWith match {
        case ALL => resolve(condition, mergeInto)
        case SOURCE_ONLY => resolve(condition, Project(Nil, mergeInto.sourceTable))
        case TARGET_ONLY => resolve(condition, Project(Nil, mergeInto.targetTable))
      }
    }

    override def resolveAssignment(
        assignment: Assignment,
        mergeInto: MergeIntoTable,
        resolvedWith: ResolvedWith): Assignment = {
      val resolvedKey = resolve(assignment.key, Project(Nil, mergeInto.targetTable))
      val resolvedValue = resolvedWith match {
        case ALL => resolve(assignment.value, mergeInto)
        case SOURCE_ONLY => resolve(assignment.value, Project(Nil, mergeInto.sourceTable))
        case TARGET_ONLY => resolve(assignment.value, Project(Nil, mergeInto.targetTable))
      }
      Assignment(resolvedKey, resolvedValue)
    }
  }

}
