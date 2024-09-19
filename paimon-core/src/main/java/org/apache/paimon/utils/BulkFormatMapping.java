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

package org.apache.paimon.utils;

import org.apache.paimon.casting.CastFieldGetter;
import org.apache.paimon.format.FileFormatDiscover;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.partition.PartitionUtils;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.schema.IndexCastMapping;
import org.apache.paimon.schema.SchemaEvolutionUtil;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.paimon.predicate.PredicateBuilder.excludePredicateWithFields;

/** Class with index mapping and bulk format. */
public class BulkFormatMapping {

    @Nullable private final int[] indexMapping;
    @Nullable private final CastFieldGetter[] castMapping;
    @Nullable private final Pair<int[], RowType> partitionPair;
    private final FormatReaderFactory bulkFormat;
    private final TableSchema dataSchema;
    private final List<Predicate> dataFilters;

    public BulkFormatMapping(
            @Nullable int[] indexMapping,
            @Nullable CastFieldGetter[] castMapping,
            @Nullable Pair<int[], RowType> partitionPair,
            FormatReaderFactory bulkFormat,
            TableSchema dataSchema,
            List<Predicate> dataFilters) {
        this.indexMapping = indexMapping;
        this.castMapping = castMapping;
        this.bulkFormat = bulkFormat;
        this.partitionPair = partitionPair;
        this.dataSchema = dataSchema;
        this.dataFilters = dataFilters;
    }

    @Nullable
    public int[] getIndexMapping() {
        return indexMapping;
    }

    @Nullable
    public CastFieldGetter[] getCastMapping() {
        return castMapping;
    }

    @Nullable
    public Pair<int[], RowType> getPartitionPair() {
        return partitionPair;
    }

    public FormatReaderFactory getReaderFactory() {
        return bulkFormat;
    }

    public TableSchema getDataSchema() {
        return dataSchema;
    }

    public List<Predicate> getDataFilters() {
        return dataFilters;
    }

    /** Builder for {@link BulkFormatMapping}. */
    public static class BulkFormatMappingBuilder {

        private final FileFormatDiscover formatDiscover;
        private final List<DataField> requiredTableFields;
        private final Function<TableSchema, List<DataField>> fieldsExtractor;
        @Nullable private final List<Predicate> filters;

        public BulkFormatMappingBuilder(
                FileFormatDiscover formatDiscover,
                List<DataField> requiredTableFields,
                Function<TableSchema, List<DataField>> fieldsExtractor,
                @Nullable List<Predicate> filters) {
            this.formatDiscover = formatDiscover;
            this.requiredTableFields = requiredTableFields;
            this.fieldsExtractor = fieldsExtractor;
            this.filters = filters;
        }

        public BulkFormatMapping build(
                String formatIdentifier, TableSchema tableSchema, TableSchema dataSchema) {

            Set<Integer> requiredFieldIds =
                    requiredTableFields.stream().map(DataField::id).collect(Collectors.toSet());
            List<DataField> requiredDataFields =
                    fieldsExtractor.apply(dataSchema).stream()
                            .filter(field -> requiredFieldIds.contains(field.id()))
                            .collect(Collectors.toList());

            IndexCastMapping indexCastMapping =
                    SchemaEvolutionUtil.createIndexCastMapping(
                            requiredTableFields, requiredDataFields);

            Pair<Pair<int[], RowType>, List<DataField>> partitionMappingAndFieldsWithoutPartition =
                    PartitionUtils.constructPartitionMapping(dataSchema, requiredDataFields);
            Pair<int[], RowType> partitionMapping =
                    partitionMappingAndFieldsWithoutPartition.getLeft();

            RowType requiredDataRowType =
                    new RowType(partitionMappingAndFieldsWithoutPartition.getRight());

            List<Predicate> readFilters = readFilters(filters, tableSchema, dataSchema);

            return new BulkFormatMapping(
                    indexCastMapping.getIndexMapping(),
                    indexCastMapping.getCastMapping(),
                    partitionMapping,
                    formatDiscover
                            .discover(formatIdentifier)
                            .createReaderFactory(requiredDataRowType, readFilters),
                    dataSchema,
                    readFilters);
        }

        private List<Predicate> readFilters(
                List<Predicate> filters, TableSchema tableSchema, TableSchema dataSchema) {
            List<Predicate> dataFilters =
                    tableSchema.id() == dataSchema.id()
                            ? filters
                            : SchemaEvolutionUtil.createDataFilters(
                                    tableSchema.fields(), dataSchema.fields(), filters);

            // Skip pushing down partition filters to reader
            return excludePredicateWithFields(
                    dataFilters, new HashSet<>(dataSchema.partitionKeys()));
        }
    }
}
