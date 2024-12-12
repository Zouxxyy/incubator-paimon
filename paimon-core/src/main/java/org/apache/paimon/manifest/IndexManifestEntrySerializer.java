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

package org.apache.paimon.manifest;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexFileMetaSerializer;
import org.apache.paimon.utils.VersionedObjectSerializer;

import static org.apache.paimon.utils.SerializationUtils.deserializeBinaryRow;
import static org.apache.paimon.utils.SerializationUtils.serializeBinaryRow;

/** A {@link VersionedObjectSerializer} for {@link IndexManifestEntry}. */
public class IndexManifestEntrySerializer extends VersionedObjectSerializer<IndexManifestEntry> {

    private final IndexFileMetaSerializer indexFileMetaSerializer;

    public IndexManifestEntrySerializer() {
        super(IndexManifestEntry.SCHEMA);
        this.indexFileMetaSerializer = new IndexFileMetaSerializer();
    }

    @Override
    public int getVersion() {
        return 2;
    }

    @Override
    public InternalRow convertTo(IndexManifestEntry record) {
        IndexFileMeta indexFile = record.indexFile();
        return GenericRow.of(
                record.kind().toByteValue(),
                serializeBinaryRow(record.partition()),
                record.bucket(),
                indexFileMetaSerializer.toRow(indexFile));
    }

    @Override
    public IndexManifestEntry convertFrom(int version, InternalRow row) {
        if (version != 2) {
            throw new UnsupportedOperationException("Unsupported version: " + version);
        }

        // here ?!!!
        return new IndexManifestEntry(
                FileKind.fromByteValue(row.getByte(0)),
                deserializeBinaryRow(row.getBinary(1)),
                row.getInt(2),
                indexFileMetaSerializer.fromRow(
                        row.getRow(3, indexFileMetaSerializer.numFields())));
    }
}
