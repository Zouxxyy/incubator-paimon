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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical._

trait PaimonMergeIntoResolverBase {

  protected trait MergeExpressionResolver {

    def resolveCondition(
        condition: Expression,
        mergeInto: MergeIntoTable,
        resolvedWith: ResolvedWith): Expression

    def resolveAssignment(
        assignment: Assignment,
        mergeInto: MergeIntoTable,
        resolvedWith: ResolvedWith): Assignment
  }

  protected def newResolver(spark: SparkSession): MergeExpressionResolver

  def apply(merge: MergeIntoTable, spark: SparkSession): LogicalPlan = {
    val target = merge.targetTable
    val source = merge.sourceTable
    assert(target.resolved, "Target should have been resolved here.")
    assert(source.resolved, "Source should have been resolved here.")

    val resolve = newResolver(spark)

    val resolvedCond = resolveCondition(resolve, merge.mergeCondition, merge, ALL)
    val resolvedMatched = resolveMatchedByTargetActions(merge, resolve)
    val resolvedNotMatched = resolveNotMatchedByTargetActions(merge, resolve)
    val resolvedNotMatchedBySource = resolveNotMatchedBySourceActions(merge, resolve)

    build(merge, resolvedCond, resolvedMatched, resolvedNotMatched, resolvedNotMatchedBySource)
  }

  def build(
      merge: MergeIntoTable,
      resolvedCond: Expression,
      resolvedMatched: Seq[MergeAction],
      resolvedNotMatched: Seq[MergeAction],
      resolvedNotMatchedBySource: Seq[MergeAction]): MergeIntoTable

  private def resolveMatchedByTargetActions(
      merge: MergeIntoTable,
      resolve: MergeExpressionResolver): Seq[MergeAction] = {
    merge.matchedActions.map {
      case DeleteAction(condition) =>
        // The condition can be from both target and source tables
        val resolvedCond = condition.map(resolveCondition(resolve, _, merge, ALL))
        DeleteAction(resolvedCond)
      case UpdateAction(condition, assignments) =>
        // The condition and value can be from both target and source tables
        val resolvedCond = condition.map(resolveCondition(resolve, _, merge, ALL))
        val resolvedAssignments = resolveAssignments(resolve, assignments, merge, ALL)
        UpdateAction(resolvedCond, resolvedAssignments)
      case UpdateStarAction(condition) =>
        // The condition can be from both target and source tables, but the value must be from the source table
        val resolvedCond = condition.map(resolveCondition(resolve, _, merge, ALL))
        val assignments = merge.targetTable.output.map {
          attr => Assignment(attr, UnresolvedAttribute(Seq(attr.name)))
        }
        val resolvedAssignments =
          resolveAssignments(resolve, assignments, merge, SOURCE_ONLY)
        UpdateAction(resolvedCond, resolvedAssignments)
      case action =>
        throw new RuntimeException(s"Can't recognize this action: $action")
    }
  }

  private def resolveNotMatchedByTargetActions(
      merge: MergeIntoTable,
      resolve: MergeExpressionResolver): Seq[MergeAction] = {
    merge.notMatchedActions.map {
      case InsertAction(condition, assignments) =>
        // The condition and value must be from the source table
        val resolvedCond =
          condition.map(resolveCondition(resolve, _, merge, SOURCE_ONLY))
        val resolvedAssignments =
          resolveAssignments(resolve, assignments, merge, SOURCE_ONLY)
        InsertAction(resolvedCond, resolvedAssignments)
      case InsertStarAction(condition) =>
        // The condition and value must be from the source table
        val resolvedCond =
          condition.map(resolveCondition(resolve, _, merge, SOURCE_ONLY))
        val assignments = merge.targetTable.output.map {
          attr => Assignment(attr, UnresolvedAttribute(Seq(attr.name)))
        }
        val resolvedAssignments =
          resolveAssignments(resolve, assignments, merge, SOURCE_ONLY)
        InsertAction(resolvedCond, resolvedAssignments)
      case action =>
        throw new RuntimeException(s"Can't recognize this action: $action")
    }
  }

  protected def resolveNotMatchedBySourceActions(
      merge: MergeIntoTable,
      resolve: MergeExpressionResolver): Seq[MergeAction]

  sealed trait ResolvedWith
  case object ALL extends ResolvedWith
  case object SOURCE_ONLY extends ResolvedWith
  case object TARGET_ONLY extends ResolvedWith

  protected def resolveCondition(
      resolver: MergeExpressionResolver,
      condition: Expression,
      mergeInto: MergeIntoTable,
      resolvedWith: ResolvedWith): Expression = {
    resolver.resolveCondition(condition, mergeInto, resolvedWith)
  }

  protected def resolveAssignments(
      resolver: MergeExpressionResolver,
      assignments: Seq[Assignment],
      mergeInto: MergeIntoTable,
      resolvedWith: ResolvedWith): Seq[Assignment] = {
    assignments.map(resolver.resolveAssignment(_, mergeInto, resolvedWith))
  }
}
