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

package org.apache.paimon.spark.util

import org.apache.paimon.CoreOptions
import org.apache.paimon.data.variant.{GenericVariant, PaimonShreddingUtils}
import org.apache.paimon.types.{DataField, DataType, RowType, VariantType}
import org.apache.paimon.utils.JsonSerdeUtil

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.unsafe.types.VariantVal

import java.util.{ArrayList => JArrayList, HashMap => JHashMap, Map => JMap}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object ShreddingUtils extends Logging {

  def shreddingSampleColIds(rowType: RowType, options: CoreOptions): Array[Int] = {
    val buffer = new ArrayBuffer[Int]()
    if (options.variantShreddingSampleEnabled()) {
      rowType.getFields.asScala.zipWithIndex.foreach {
        case (field, idx) if options.shreddingSchema(field.name) == null =>
          field.`type`() match {
            case _: VariantType =>
              buffer.append(idx)
            case _ =>
          }
      }
    }
    buffer.toArray
  }

  // id -> shreddingSchema
  def getShreddingSchemasBySampling(
      lst: List[Row],
      samplingRatio: Double,
      maxColumns: Int,
      ids: Array[Int]): Map[Int, RowType] = {
    val counts = new JHashMap[Integer, JMap[String, JMap[DataType, Integer]]]
    lst
      .foreach(
        row => {
          ids
            .filter(id => !row.isNullAt(id))
            .foreach(
              id => {
                val v = row.get(id).asInstanceOf[VariantVal]
                val paimonVariant: GenericVariant = new GenericVariant(v.getValue, v.getMetadata)
                val value: JMap[String, DataType] = paimonVariant.getObjectKeyValueType()
                value.forEach(
                  (key, value) => {
                    val countMap =
                      counts.getOrDefault(id, new JHashMap[String, JMap[DataType, Integer]]())
                    val typeCountMap = countMap.getOrDefault(key, new JHashMap[DataType, Integer]())
                    typeCountMap.put(value, typeCountMap.getOrDefault(value, 0) + 1)
                    countMap.put(key, typeCountMap)
                    counts.put(id, countMap)
                  })
              })
        })

    // id -> <key, <type, count>> -> id -> shreddingSchema
    val minSamplingCount = lst.size.toDouble * samplingRatio
    val res: mutable.Map[Int, RowType] = mutable.Map()
    counts.forEach(
      (id, countMap) => {
        val builder = RowType.builder()

        countMap.asScala
          .flatMap {
            case (key, typeCountMap) =>
              typeCountMap.asScala.toSeq
                .sortBy { case (_, count) => -count }
                .map(a => (key, a._1, a._2))
          }
          .toSeq
          .filter(x => x._3 > minSamplingCount)
          .sortBy(-_._3)
          .take(maxColumns)
          .foreach(
            x => {
              builder.field(x._1, x._2)
            })

        val rowType = builder.build()
        if (rowType.getFields.size() > 0) {
          logWarning(s"Sample shredding schema for field id $id, field size ${rowType.getFields
              .size()}, schema: ${JsonSerdeUtil.toJson(rowType)}")
          res.put(id, PaimonShreddingUtils.variantShreddingSchema(rowType))
        }
      })

    res.toMap
  }

  def setShreddingSchemas(rowType: RowType, shreddingSchemas: Map[Int, RowType]): RowType = {
    val newFields = new JArrayList[DataField](rowType.getFields.size)
    rowType.getFields.asScala.zipWithIndex.foreach {
      case (field, idx) =>
        field.`type`() match {
          case _: VariantType if shreddingSchemas.contains(idx) =>
            val v = field.`type`.copy.asInstanceOf[VariantType]
            v.setShreddingSchema(shreddingSchemas(idx))
            newFields.add(field.newType(v))
          case _ =>
            newFields.add(field)
        }
    }
    rowType.copy(newFields)
  }
}
