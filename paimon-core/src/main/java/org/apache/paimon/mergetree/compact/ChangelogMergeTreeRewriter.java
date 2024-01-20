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
import org.apache.paimon.mergetree.MergeSorter;
import org.apache.paimon.mergetree.MergeTreeReaders;
import org.apache.paimon.mergetree.SortedRun;
import org.apache.paimon.reader.RecordReaderIterator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * A {@link MergeTreeCompactRewriter} which produces changelog files while performing compaction.
 */
public abstract class ChangelogMergeTreeRewriter extends MergeTreeCompactRewriter {

    protected final RecordEqualiser valueEqualiser;
    protected final boolean changelogRowDeduplicate;

    public ChangelogMergeTreeRewriter(
            KeyValueFileReaderFactory readerFactory,
            KeyValueFileWriterFactory writerFactory,
            Comparator<InternalRow> keyComparator,
            MergeFunctionFactory<KeyValue> mfFactory,
            MergeSorter mergeSorter,
            RecordEqualiser valueEqualiser,
            boolean changelogRowDeduplicate) {
        super(readerFactory, writerFactory, keyComparator, mfFactory, mergeSorter);
        this.valueEqualiser = valueEqualiser;
        this.changelogRowDeduplicate = changelogRowDeduplicate;
    }

    protected abstract boolean needRewriteWithChangelog(
            int outputLevel, boolean dropDelete, List<List<SortedRun>> sections);

    protected abstract boolean needUpgradeWithChangelog(int outputLevel, DataFileMeta file);

    protected abstract MergeFunctionWrapper<ChangelogResult> createMergeWrapper(int outputLevel);

    protected boolean rewriteLookupChangelog(int outputLevel, List<List<SortedRun>> sections) {
        if (outputLevel == 0) {
            return false;
        }

        for (List<SortedRun> runs : sections) {
            for (SortedRun run : runs) {
                for (DataFileMeta file : run.files()) {
                    if (file.level() == 0) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    @Override
    public CompactResult rewrite(
            int outputLevel, boolean dropDelete, List<List<SortedRun>> sections) throws Exception {
        if (needRewriteWithChangelog(outputLevel, dropDelete, sections)) {
            return rewriteWithChangelog(outputLevel, sections);
        } else {
            return super.rewrite(outputLevel, dropDelete, sections);
        }
    }

    /** Rewrite and produce changelog at the same time. */
    private CompactResult rewriteWithChangelog(int outputLevel, List<List<SortedRun>> sections)
            throws Exception {
        List<ConcatRecordReader.ReaderSupplier<ChangelogResult>> sectionReaders = new ArrayList<>();
        for (List<SortedRun> section : sections) {
            sectionReaders.add(
                    () ->
                            MergeTreeReaders.readerForSection(
                                    section,
                                    readerFactory,
                                    keyComparator,
                                    createMergeWrapper(outputLevel),
                                    mergeSorter));
        }

        try (RecordReaderIterator<ChangelogResult> iterator =
                        new RecordReaderIterator<>(ConcatRecordReader.create(sectionReaders));
                RollingFileWriter<KeyValue, DataFileMeta> compactFileWriter =
                        writerFactory.createRollingMergeTreeFileWriter(outputLevel);
                RollingFileWriter<KeyValue, DataFileMeta> changelogFileWriter =
                        writerFactory.createRollingChangelogFileWriter(outputLevel)) {
            while (iterator.hasNext()) {
                ChangelogResult result = iterator.next();
                if (result.result() != null) {
                    compactFileWriter.write(result.result());
                }
                changelogFileWriter.write(result.changelogs());
            }
            return new CompactResult(
                    extractFilesFromSections(sections),
                    compactFileWriter.result(),
                    changelogFileWriter.result());
        }
    }

    @Override
    public CompactResult upgrade(int outputLevel, DataFileMeta file) throws Exception {
        if (needUpgradeWithChangelog(outputLevel, file)) {
            return upgradeWithChangelog(outputLevel, file);
        } else {
            return super.upgrade(outputLevel, file);
        }
    }

    /**
     * Even for upgrade, we still need performing rewrite (except for {@link
     * MergeEngine#DEDUPLICATE}).
     */
    protected CompactResult upgradeWithChangelog(int outputLevel, DataFileMeta file)
            throws Exception {
        return rewriteWithChangelog(
                outputLevel,
                Collections.singletonList(Collections.singletonList(SortedRun.fromSingle(file))));
    }
}
