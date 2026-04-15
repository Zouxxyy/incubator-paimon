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

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.analysis.ColumnResolutionHelper
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.{Assignment, DeleteAction, LogicalPlan, MergeAction, MergeIntoTable, Project, UpdateAction}
import org.apache.spark.sql.catalyst.util.ResolveDefaultColumns.resolveColumnDefaultInAssignmentValue
import org.apache.spark.sql.catalyst.util.quoteIdentifier
import org.apache.spark.sql.internal.SQLConf

/** Resolve all the expressions for MergeInto. */
object PaimonMergeIntoResolver extends PaimonMergeIntoResolverBase {

  override protected def newResolver(spark: SparkSession): MergeExpressionResolver = {
    new SparkMergeExpressionResolver(spark)
  }

  override protected def resolveNotMatchedBySourceActions(
      merge: MergeIntoTable,
      resolve: MergeExpressionResolver): Seq[MergeAction] = {
    merge.notMatchedBySourceActions.map {
      case DeleteAction(condition) =>
        // The condition must be from the target table
        val resolvedCond = condition.map(resolveCondition(resolve, _, merge, TARGET_ONLY))
        DeleteAction(resolvedCond)
      case UpdateAction(condition, assignments) =>
        // The condition and value must be from the target table
        val resolvedCond = condition.map(resolveCondition(resolve, _, merge, TARGET_ONLY))
        val resolvedAssignments = resolveAssignments(resolve, assignments, merge, TARGET_ONLY)
        UpdateAction(resolvedCond, resolvedAssignments)
      case action =>
        throw new RuntimeException(s"Can't recognize this action: $action")
    }
  }

  def build(
      merge: MergeIntoTable,
      resolvedCond: Expression,
      resolvedMatched: Seq[MergeAction],
      resolvedNotMatched: Seq[MergeAction],
      resolvedNotMatchedBySource: Seq[MergeAction]): MergeIntoTable = {
    merge.copy(
      mergeCondition = resolvedCond,
      matchedActions = resolvedMatched,
      notMatchedActions = resolvedNotMatched,
      notMatchedBySourceActions = resolvedNotMatchedBySource
    )
  }

  final private class SparkMergeExpressionResolver(spark: SparkSession)
    extends MergeExpressionResolver
    with ColumnResolutionHelper {

    override def conf: SQLConf = spark.sessionState.conf

    override def resolveCondition(
        condition: Expression,
        mergeInto: MergeIntoTable,
        resolvedWith: ResolvedWith): Expression = {
      val (resolvedCondition, resolvePlan) = resolvedWith match {
        case ALL =>
          resolveExpressionByPlanChildren(condition, mergeInto) -> mergeInto
        case SOURCE_ONLY =>
          resolveExpressionByPlanOutput(condition, mergeInto.sourceTable) ->
            Project(Nil, mergeInto.sourceTable)
        case TARGET_ONLY =>
          resolveExpressionByPlanOutput(condition, mergeInto.targetTable) ->
            Project(Nil, mergeInto.targetTable)
      }
      checkResolvedMergeExpr(resolvedCondition, resolvePlan)
      resolvedCondition
    }

    override def resolveAssignment(
        assignment: Assignment,
        mergeInto: MergeIntoTable,
        resolvedWith: ResolvedWith): Assignment = {
      val keyPlan = Project(Nil, mergeInto.targetTable)
      val resolvedKey = resolveMergeExprOrFail(assignment.key, keyPlan)
      val valuePlan = resolvedWith match {
        case ALL => mergeInto
        case SOURCE_ONLY => Project(Nil, mergeInto.sourceTable)
        case TARGET_ONLY => Project(Nil, mergeInto.targetTable)
      }
      val resolvedValue = resolveAssignmentValue(resolvedKey, assignment.value, valuePlan)
      Assignment(resolvedKey, resolvedValue)
    }

    private def resolveAssignmentValue(
        key: Expression,
        value: Expression,
        resolvePlan: LogicalPlan): Expression = {
      val resolvedExpr =
        if (value.resolved) {
          value
        } else {
          resolveExprInAssignment(value, resolvePlan)
        }
      val defaultResolvedExpr =
        if (conf.enableDefaultColumns) {
          resolveColumnDefaultInAssignmentValue(key, resolvedExpr, invalidDefaultReferenceInMerge())
        } else {
          resolvedExpr
        }
      checkResolvedMergeExpr(defaultResolvedExpr, resolvePlan)
      defaultResolvedExpr
    }

    private def resolveMergeExprOrFail(expr: Expression, resolvePlan: LogicalPlan): Expression = {
      val resolvedExpr =
        if (expr.resolved) {
          expr
        } else {
          resolveExprInAssignment(expr, resolvePlan)
        }
      checkResolvedMergeExpr(resolvedExpr, resolvePlan)
      resolvedExpr
    }

    private def checkResolvedMergeExpr(expr: Expression, resolvePlan: LogicalPlan): Unit = {
      expr.references.filter(!_.resolved).foreach {
        attr =>
          val proposal = resolvePlan.inputSet.toSeq.map(_.name)
          throw unresolvedColumnError(attr.name, proposal)
      }
    }

    private def invalidDefaultReferenceInMerge(): Throwable = {
      new AnalysisException(errorClass = "_LEGACY_ERROR_TEMP_1343", messageParameters = Map.empty)
    }

    private def unresolvedColumnError(columnName: String, proposal: Seq[String]): Throwable = {
      val messageParameters = Map("objectName" -> quoteIdentifier(columnName)) ++
        (if (proposal.isEmpty) {
           Map.empty[String, String]
         } else {
           Map("proposal" -> proposal.take(5).map(quoteIdentifier).mkString(", "))
         })
      val errorSubClass = if (proposal.isEmpty) "WITHOUT_SUGGESTION" else "WITH_SUGGESTION"
      new AnalysisException(
        errorClass = s"UNRESOLVED_COLUMN.$errorSubClass",
        messageParameters = messageParameters)
    }
  }

}
