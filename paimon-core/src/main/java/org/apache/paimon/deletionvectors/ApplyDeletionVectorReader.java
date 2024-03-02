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

package org.apache.paimon.deletionvectors;

import org.apache.paimon.KeyValue;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordWithPositionIterator;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** A {@link RecordReader} which apply {@link DeletionVector} to filter record. */
public class ApplyDeletionVectorReader implements RecordReader<KeyValue> {

    private final RecordReader<KeyValue> reader;

    private final DeletionVector deletionVector;

    public ApplyDeletionVectorReader(RecordReader<KeyValue> reader, DeletionVector deletionVector) {
        this.reader = reader;
        this.deletionVector = deletionVector;
    }

    @Nullable
    @Override
    public RecordIterator<KeyValue> readBatch() throws IOException {
        RecordIterator<KeyValue> batch = reader.readBatch();

        if (batch == null) {
            return null;
        }

        checkArgument(
                batch instanceof RecordWithPositionIterator,
                "RecordIterator in ApplyDeletionVectorReader must be RecordWithPositionIterator instead of "
                        + batch.getClass());

        RecordWithPositionIterator<KeyValue> batchWithPosition =
                (RecordWithPositionIterator<KeyValue>) batch;
        return new RecordIterator<KeyValue>() {
            @Override
            public KeyValue next() throws IOException {
                while (true) {
                    KeyValue kv = batchWithPosition.next();
                    if (kv == null) {
                        return null;
                    }
                    if (!deletionVector.isDeleted(batchWithPosition.rowPosition())) {
                        return kv;
                    }
                }
            }

            @Override
            public void releaseBatch() {
                batch.releaseBatch();
            }
        };
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
