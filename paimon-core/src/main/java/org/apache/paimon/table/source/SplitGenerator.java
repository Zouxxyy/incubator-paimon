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

package org.apache.paimon.table.source;

import org.apache.paimon.io.DataFileMeta;

import java.util.List;

/** Generate splits from {@link DataFileMeta}s. */
public interface SplitGenerator {

    List<SplitGroup> splitForBatch(List<DataFileMeta> files);

    List<SplitGroup> splitForStreaming(List<DataFileMeta> files);

    /** Split group. */
    class SplitGroup {
        final List<DataFileMeta> files;
        final boolean noMergeRead;

        public List<DataFileMeta> files() {
            return files;
        }

        public boolean noMergeRead() {
            return noMergeRead;
        }

        private SplitGroup(List<DataFileMeta> files, boolean noMergeRead) {
            this.files = files;
            this.noMergeRead = noMergeRead;
        }

        public static SplitGroup noMergeGroup(List<DataFileMeta> files) {
            return new SplitGroup(files, true);
        }

        public static SplitGroup mergeGroup(List<DataFileMeta> files) {
            return new SplitGroup(files, false);
        }
    }
}
