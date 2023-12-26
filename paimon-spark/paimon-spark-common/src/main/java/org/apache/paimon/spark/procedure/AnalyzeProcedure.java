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

package org.apache.paimon.spark.procedure;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.spark.SparkCatalog;
import org.apache.paimon.spark.catalog.WithPaimonCatalog;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataFieldStats;

import org.apache.spark.sql.Utils;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.plans.logical.ColumnStat;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.math.BigInt;

import static org.apache.spark.sql.types.DataTypes.StringType;

/**
 * Analyze procedure. Usage:
 *
 * <pre><code>
 *  CALL sys.analyze(table => 'xxx', [cols => 'co1, co2'])
 * </code></pre>
 */
public class AnalyzeProcedure extends BaseProcedure {
    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.required("table", StringType),
                ProcedureParameter.optional("cols", StringType)
            };

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField("result", DataTypes.BooleanType, true, Metadata.empty())
                    });

    protected AnalyzeProcedure(TableCatalog tableCatalog) {
        super(tableCatalog);
    }

    @Override
    public ProcedureParameter[] parameters() {
        return PARAMETERS;
    }

    @Override
    public StructType outputType() {
        return OUTPUT_TYPE;
    }

    @Override
    public InternalRow[] call(InternalRow args) {

        Identifier tableIdent = toIdentifier(args.getString(0), PARAMETERS[0].name());
        Set<String> requiredCols =
                args.isNullAt(1)
                        ? null
                        : new HashSet<>(Arrays.asList(args.getString(1).split(",")));

        FileStoreTable table = (FileStoreTable) loadSparkTable(tableIdent).getTable();

        LogicalPlan relation =
                spark().read()
                        .format("paimon")
                        .load(table.coreOptions().path().toString())
                        .logicalPlan();

        Seq<Attribute> requiredAttrs = relation.output();
        if (requiredCols != null) {
            requiredAttrs.filter(x -> requiredCols.contains(x.name()));
        }

        List<SchemaChange> schemaChanges =
                colStatsToSchemaChange(Utils.computeColumnStats(spark(), relation, requiredAttrs));
        try {
            ((WithPaimonCatalog) tableCatalog())
                    .paimonCatalog()
                    .alterTable(SparkCatalog.toIdentifier(tableIdent), schemaChanges, false);
        } catch (Catalog.TableNotExistException
                | Catalog.ColumnAlreadyExistException
                | Catalog.ColumnNotExistException
                | NoSuchTableException e) {
            throw new RuntimeException(e);
        }

        return new InternalRow[] {newInternalRow(true)};
    }

    public static ProcedureBuilder builder() {
        return new BaseProcedure.Builder<AnalyzeProcedure>() {
            @Override
            public AnalyzeProcedure doBuild() {
                return new AnalyzeProcedure(tableCatalog());
            }
        };
    }

    @Override
    public String description() {
        return "AnalyzeProcedure";
    }

    private static List<SchemaChange> colStatsToSchemaChange(
            Tuple2<Object, scala.collection.immutable.Map<Attribute, ColumnStat>> sparkColStats) {
        List<SchemaChange> changes = new ArrayList<>();
        BigInt rowCount = BigInt.apply((Long) sparkColStats._1);
        Map<Attribute, ColumnStat> map =
                JavaConverters.mapAsJavaMapConverter(sparkColStats._2()).asJava();

        for (Map.Entry<Attribute, ColumnStat> entry : map.entrySet()) {
            Attribute attribute = entry.getKey();
            ColumnStat columnStat = entry.getValue();

            Double distinctRatio =
                    columnStat.distinctCount().isDefined()
                            ? columnStat.distinctCount().get().doubleValue()
                                    / rowCount.doubleValue()
                            : null;
            Long avgLen = columnStat.avgLen().isDefined() ? (Long) columnStat.avgLen().get() : null;
            Long maxLen = columnStat.maxLen().isDefined() ? (Long) columnStat.maxLen().get() : null;

            changes.add(
                    SchemaChange.updateColumnStats(
                            attribute.name(), new DataFieldStats(distinctRatio, avgLen, maxLen)));
        }
        return changes;
    }
}
