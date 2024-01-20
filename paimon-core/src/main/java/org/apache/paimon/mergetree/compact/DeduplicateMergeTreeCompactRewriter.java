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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.CoreOptions.MergeEngine;
import org.apache.paimon.KeyValue;
import org.apache.paimon.codegen.RecordEqualiser;
import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.KeyValueFileReaderFactory;
import org.apache.paimon.io.KeyValueFileWriterFactory;
import org.apache.paimon.io.RollingFileWriter;
import org.apache.paimon.mergetree.LookupLevels;
import org.apache.paimon.mergetree.MergeSorter;
import org.apache.paimon.mergetree.MergeTreeReaders;
import org.apache.paimon.mergetree.SortedRun;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReaderIterator;

import java.util.Collections;
import java.util.Comparator;

/**
 * A {@link MergeTreeCompactRewriter} for {@link MergeEngine#DEDUPLICATE}, when performing upgrade,
 * can skip rewriting the data, only write changelog.
 */
public class DeduplicateMergeTreeCompactRewriter extends LookupMergeTreeCompactRewriter {
    public DeduplicateMergeTreeCompactRewriter(
            LookupLevels lookupLevels,
            KeyValueFileReaderFactory readerFactory,
            KeyValueFileWriterFactory writerFactory,
            Comparator<InternalRow> keyComparator,
            MergeFunctionFactory<KeyValue> mfFactory,
            MergeSorter mergeSorter,
            RecordEqualiser valueEqualiser,
            boolean changelogRowDeduplicate) {
        super(
                lookupLevels,
                readerFactory,
                writerFactory,
                keyComparator,
                mfFactory,
                mergeSorter,
                valueEqualiser,
                changelogRowDeduplicate);
    }

    /** Skip rewrite new file, only update file metadata, and produce changelog. */
    @Override
    protected CompactResult upgradeWithChangelog(int outputLevel, DataFileMeta file)
            throws Exception {
        RecordReader<ChangelogResult> sectionReader =
                MergeTreeReaders.readerForSection(
                        Collections.singletonList(SortedRun.fromSingle(file)),
                        readerFactory,
                        keyComparator,
                        createMergeWrapper(outputLevel),
                        mergeSorter);

        try (RecordReaderIterator<ChangelogResult> iterator =
                        new RecordReaderIterator<>(sectionReader);
                RollingFileWriter<KeyValue, DataFileMeta> changelogFileWriter =
                        writerFactory.createRollingChangelogFileWriter(outputLevel)) {
            while (iterator.hasNext()) {
                ChangelogResult result = iterator.next();
                changelogFileWriter.write(result.changelogs());
            }
            return new CompactResult(file, file.upgrade(outputLevel), changelogFileWriter.result());
        }
    }
}
