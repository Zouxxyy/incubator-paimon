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
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import scala.Option;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.Seq;

/**
 * Analyze procedure. Usage:
 *
 * <pre><code>
 *  CALL sys.analyze(table => 'table_name', [cols => 'co1,co2'])
 * </code></pre>
 */
public class AnalyzeProcedure extends BaseProcedure {

    private static final Set<Class<? extends DataType>> SUPPORTED_TYPES = new HashSet<>();

    static {
        SUPPORTED_TYPES.add(org.apache.spark.sql.types.IntegralType.class);
        SUPPORTED_TYPES.add(org.apache.spark.sql.types.DecimalType.class);
        SUPPORTED_TYPES.add(org.apache.spark.sql.types.DoubleType$.class);
        SUPPORTED_TYPES.add(org.apache.spark.sql.types.FloatType$.class);
        SUPPORTED_TYPES.add(org.apache.spark.sql.types.BooleanType$.class);
        SUPPORTED_TYPES.add(org.apache.spark.sql.types.DateType$.class);
        SUPPORTED_TYPES.add(org.apache.spark.sql.types.TimestampType$.class);
        SUPPORTED_TYPES.add(org.apache.spark.sql.types.BinaryType$.class);
        SUPPORTED_TYPES.add(org.apache.spark.sql.types.StringType$.class);
    }

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.required("table", DataTypes.StringType),
                ProcedureParameter.optional("cols", DataTypes.StringType)
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
            requiredAttrs = requiredAttrs.filter(x -> requiredCols.contains(x.name())).toSeq();
        }

        Option<Attribute> unsupportedAttr =
                requiredAttrs.find(
                        attr ->
                                SUPPORTED_TYPES.stream()
                                        .noneMatch(clazz -> clazz.isInstance(attr.dataType())));
        if (unsupportedAttr.isDefined()) {
            Attribute attr = unsupportedAttr.get();
            throw new RuntimeException(
                    String.format(
                            "Analyze on col `%s` of type %s is not supported",
                            attr.name(), attr.dataType()));
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

    private List<SchemaChange> colStatsToSchemaChange(
            Tuple2<Object, scala.collection.immutable.Map<Attribute, ColumnStat>> sparkColStats) {
        return JavaConverters.mapAsJavaMapConverter(sparkColStats._2()).asJava().entrySet().stream()
                .map(
                        entry ->
                                SchemaChange.updateColumnStats(
                                        entry.getKey().name(),
                                        sparkColumnStatsToPaimon(entry.getValue())))
                .collect(Collectors.toList());
    }

    private DataFieldStats sparkColumnStatsToPaimon(ColumnStat columnStat) {
        return new DataFieldStats(
                columnStat.distinctCount().isDefined()
                        ? columnStat.distinctCount().get().longValue()
                        : null,
                columnStat.nullCount().isDefined()
                        ? columnStat.nullCount().get().longValue()
                        : null,
                columnStat.avgLen().isDefined() ? (Long) columnStat.avgLen().get() : null,
                columnStat.maxLen().isDefined() ? (Long) columnStat.maxLen().get() : null);
    }
}
