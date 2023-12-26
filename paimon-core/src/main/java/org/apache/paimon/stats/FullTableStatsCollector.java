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

package org.apache.paimon.stats;

import org.apache.paimon.format.FieldStats;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

/** Collector for {@link FullTableStats}. */
public class FullTableStatsCollector {
    private final FieldStatsArraySerializer serializer;
    private final List<DataField> requiredFields;
    private final HashMap<DataField, FullTableStats> statsMap = new HashMap<>();

    public FullTableStatsCollector(RowType rowType) {
        this(rowType, rowType.getFieldNames());
    }

    public FullTableStatsCollector(RowType rowType, List<String> requiredFields) {
        serializer = new FieldStatsArraySerializer(rowType);
        this.requiredFields =
                rowType.getFields().stream()
                        .filter(field -> requiredFields.contains(field.name()))
                        .collect(Collectors.toList());
        for (DataField requiredField : this.requiredFields) {
            statsMap.put(
                    requiredField, new FullTableStats(requiredField.stats(), FieldStats.empty()));
        }
    }

    public void collect(BinaryTableStats valueStats) {
        FieldStats[] allFieldStats = valueStats.fields(serializer);
        for (DataField requiredField : requiredFields) {
            statsMap.get(requiredField)
                    .fileLevelFieldStats()
                    .merge(allFieldStats[requiredField.id()]);
        }
    }

    public HashMap<DataField, FullTableStats> extract() {
        return statsMap;
    }
}
