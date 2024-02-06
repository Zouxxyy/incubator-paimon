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

package org.apache.paimon.index.delete;

import org.apache.paimon.KeyValue;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexMaintainer;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** x. */
public class DeleteMapIndexMaintainer implements IndexMaintainer<KeyValue, DeleteIndex> {

    private final IndexFileHandler indexFileHandler;
    private final IndexFileMeta indexFile;
    private Map<String, DeleteIndex> deleteMap;
    private boolean modified;
    private boolean restored;
    private final HashSet<String> restoredFile;

    public DeleteMapIndexMaintainer(
            IndexFileHandler fileHandler,
            @Nullable Long snapshotId,
            BinaryRow partition,
            int bucket) {
        this.indexFileHandler = fileHandler;
        this.indexFile =
                snapshotId == null
                        ? null
                        : fileHandler
                                .scan(
                                        snapshotId,
                                        DeleteMapIndexFile.DELETE_MAP_INDEX,
                                        partition,
                                        bucket)
                                .orElse(null);
        this.deleteMap = new HashMap<>();
        this.modified = false;
        this.restored = false;
        this.restoredFile = new HashSet<>();
    }

    @Override
    public void notifyNewRecord(KeyValue record) {
        restoreDeleteMap();
        DeleteIndex deleteIndex =
                deleteMap.computeIfAbsent(record.fileName(), k -> new BitmapDeleteIndex());
        if (!deleteIndex.isDeleted(record.position())) {
            deleteIndex.delete(record.position());
            modified = true;
        }
    }

    public void delete(String fileName) {
        restoreDeleteMap();
        if (deleteMap.containsKey(fileName)) {
            deleteMap.remove(fileName);
            modified = true;
        }
    }

    @Override
    public List<IndexFileMeta> prepareCommit() {
        if (modified) {
            IndexFileMeta entry = indexFileHandler.writeDeleteMapIndex(deleteMap);
            modified = false;
            return Collections.singletonList(entry);
        }
        return Collections.emptyList();
    }

    // This method is only used by writer, which restore the whole delete map
    private void restoreDeleteMap() {
        if (indexFile != null && !restored) {
            this.deleteMap = indexFileHandler.readDeleteMapIndex(indexFile);
            restored = true;
        }
    }

    // This method is only used by the reader, which just lazy load the specified delete index
    public Optional<DeleteIndex> indexOf(String fileName) {
        if (indexFile != null
                && !restored
                && !restoredFile.contains(fileName)
                && !deleteMap.containsKey(fileName)) {
            restoredFile.add(fileName);
            indexFileHandler
                    .readDeleteIndex(indexFile, fileName)
                    .ifPresent(deleteIndex -> deleteMap.put(fileName, deleteIndex));
        }
        return Optional.ofNullable(deleteMap.get(fileName));
    }

    /** Factory to restore {@link DeleteMapIndexMaintainer}. */
    public static class Factory implements IndexMaintainer.Factory<KeyValue, DeleteIndex> {

        private final IndexFileHandler handler;

        public Factory(IndexFileHandler handler) {
            this.handler = handler;
        }

        @Override
        public IndexMaintainer<KeyValue, DeleteIndex> createOrRestore(
                @Nullable Long snapshotId, BinaryRow partition, int bucket) {
            return new DeleteMapIndexMaintainer(handler, snapshotId, partition, bucket);
        }
    }
}
