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
import org.apache.paimon.index.IndexMaintainer;
import org.apache.paimon.index.delete.DeleteIndex;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.KeyValueFileReaderFactory;
import org.apache.paimon.io.KeyValueFileWriterFactory;
import org.apache.paimon.io.RollingFileWriter;
import org.apache.paimon.mergetree.MergeSorter;
import org.apache.paimon.mergetree.MergeTreeReaders;
import org.apache.paimon.mergetree.SortedRun;
import org.apache.paimon.reader.RecordReaderIterator;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * A {@link MergeTreeCompactRewriter} which produces changelog files while performing compaction.
 */
public abstract class ChangelogMergeTreeRewriter extends MergeTreeCompactRewriter {

    protected final int maxLevel;
    protected final MergeEngine mergeEngine;
    protected final RecordEqualiser valueEqualiser;
    protected final boolean changelogRowDeduplicate;
    protected final @Nullable IndexMaintainer<KeyValue, DeleteIndex> deleteMapMaintainer;
    protected final boolean writeChangelog;

    public ChangelogMergeTreeRewriter(
            int maxLevel,
            MergeEngine mergeEngine,
            KeyValueFileReaderFactory readerFactory,
            KeyValueFileWriterFactory writerFactory,
            Comparator<InternalRow> keyComparator,
            MergeFunctionFactory<KeyValue> mfFactory,
            MergeSorter mergeSorter,
            RecordEqualiser valueEqualiser,
            boolean changelogRowDeduplicate,
            @Nullable IndexMaintainer<KeyValue, DeleteIndex> deleteMapMaintainer,
            boolean writeChangelog) {
        super(readerFactory, writerFactory, keyComparator, mfFactory, mergeSorter);
        checkArgument(
                writeChangelog || deleteMapMaintainer != null,
                "Either writeChangelog must be true, or deleteMapMaintainer must not be null.");
        this.maxLevel = maxLevel;
        this.mergeEngine = mergeEngine;
        this.valueEqualiser = valueEqualiser;
        this.changelogRowDeduplicate = changelogRowDeduplicate;
        this.deleteMapMaintainer = deleteMapMaintainer;
        this.writeChangelog = writeChangelog;
    }

    protected abstract boolean rewriteChangelog(
            int outputLevel, boolean dropDelete, List<List<SortedRun>> sections);

    protected abstract UpgradeStrategy upgradeChangelog(int outputLevel, DataFileMeta file);

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
        if (rewriteChangelog(outputLevel, dropDelete, sections)) {
            return rewriteChangelogCompaction(outputLevel, sections, dropDelete, true);
        } else {
            return rewriteCompaction(outputLevel, dropDelete, sections);
        }
    }

    /**
     * Rewrite and produce changelog at the same time.
     *
     * @param rewriteCompactFile whether to rewrite compact file
     */
    private CompactResult rewriteChangelogCompaction(
            int outputLevel,
            List<List<SortedRun>> sections,
            boolean dropDelete,
            boolean rewriteCompactFile)
            throws Exception {
        if (deleteMapMaintainer != null) {
            dropDelete = true;
        }

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

        RecordReaderIterator<ChangelogResult> iterator = null;
        RollingFileWriter<KeyValue, DataFileMeta> compactFileWriter = null;
        RollingFileWriter<KeyValue, DataFileMeta> changelogFileWriter = null;

        try {
            iterator = new RecordReaderIterator<>(ConcatRecordReader.create(sectionReaders));
            if (rewriteCompactFile) {
                compactFileWriter = writerFactory.createRollingMergeTreeFileWriter(outputLevel);
            }
            if (writeChangelog) {
                changelogFileWriter = writerFactory.createRollingChangelogFileWriter(outputLevel);
            }

            while (iterator.hasNext()) {
                ChangelogResult result = iterator.next();
                KeyValue keyValue = result.result();
                if (rewriteCompactFile && keyValue != null && (!dropDelete || keyValue.isAdd())) {
                    compactFileWriter.write(keyValue);
                }
                if (writeChangelog) {
                    for (KeyValue kv : result.changelogs()) {
                        changelogFileWriter.write(kv);
                    }
                }
            }
        } finally {
            if (iterator != null) {
                iterator.close();
            }
            if (compactFileWriter != null) {
                compactFileWriter.close();
            }
            if (changelogFileWriter != null) {
                changelogFileWriter.close();
            }
        }

        List<DataFileMeta> before = extractFilesFromSections(sections);
        List<DataFileMeta> after =
                rewriteCompactFile
                        ? compactFileWriter.result()
                        : before.stream()
                                .map(x -> x.upgrade(outputLevel))
                                .collect(Collectors.toList());

        if (deleteMapMaintainer != null) {
            for (DataFileMeta dataFileMeta : before) {
                deleteMapMaintainer.delete(dataFileMeta.fileName());
            }
        }
        return new CompactResult(
                before,
                after,
                writeChangelog ? changelogFileWriter.result() : Collections.emptyList());
    }

    @Override
    public CompactResult upgrade(int outputLevel, DataFileMeta file) throws Exception {
        UpgradeStrategy strategy = upgradeChangelog(outputLevel, file);
        if (strategy.changelog) {
            return rewriteChangelogCompaction(
                    outputLevel,
                    Collections.singletonList(
                            Collections.singletonList(SortedRun.fromSingle(file))),
                    false,
                    strategy.rewrite);
        } else {
            return super.upgrade(outputLevel, file);
        }
    }

    /** Strategy for upgrade. */
    protected enum UpgradeStrategy {
        NO_CHANGELOG(false, false),
        CHANGELOG_NO_REWRITE(true, false),
        CHANGELOG_WITH_REWRITE(true, true);

        private final boolean changelog;
        private final boolean rewrite;

        UpgradeStrategy(boolean changelog, boolean rewrite) {
            this.changelog = changelog;
            this.rewrite = rewrite;
        }
    }
}
