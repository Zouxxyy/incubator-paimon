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

import org.apache.paimon.Snapshot;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.utils.SnapshotManager;

import java.util.Optional;

/** Handle stats files. */
public class StatsFileHandler {

    private final SnapshotManager snapshotManager;
    private final SchemaManager schemaManager;
    private final StatsFile statsFile;

    public StatsFileHandler(
            SnapshotManager snapshotManager, SchemaManager schemaManager, StatsFile statsFile) {
        this.snapshotManager = snapshotManager;
        this.schemaManager = schemaManager;
        this.statsFile = statsFile;
    }

    /**
     * Write stats to stats file.
     *
     * @return stats file name
     */
    public String writeStats(Stats stats) {
        stats.serializeFieldsToString(schemaManager.latest().get());
        return statsFile.write(stats);
    }

    public Optional<Stats> readStats() {
        return readStats(snapshotManager.latestSnapshotId());
    }

    /**
     * Read stats for the specified snapshotId.
     *
     * @return stats
     */
    public Optional<Stats> readStats(long snapshotId) {
        Snapshot snapshot = snapshotManager.snapshot(snapshotId);
        if (snapshot.stats() == null) {
            return Optional.empty();
        } else {
            Stats stats = statsFile.read(snapshot.stats());
            stats.deserializeFieldsFromString(schemaManager.schema(snapshot.schemaId()));
            return Optional.of(stats);
        }
    }
}
