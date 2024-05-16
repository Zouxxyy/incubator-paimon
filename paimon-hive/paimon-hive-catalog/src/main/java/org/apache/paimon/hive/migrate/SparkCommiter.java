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

package org.apache.paimon.hive.migrate;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryWriter;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.migrate.FileMetaUtils;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.types.RowType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.FileUtils.COMMON_IO_FORK_JOIN_POOL;

/** 1. */
public class SparkCommiter {

    private static final Predicate<FileStatus> HIDDEN_PATH_FILTER =
            p -> !p.getPath().getName().startsWith("_") && !p.getPath().getName().startsWith(".");

    Set<String> partitionNames;

    FileStoreTable fileStoreTable;

    public SparkCommiter(Set<String> partitionNames, FileStoreTable fileStoreTable) {
        this.partitionNames = partitionNames;
        this.fileStoreTable = fileStoreTable;
    }

    public List<CommitMessage> generatorCommitMessage() throws Exception {
        FileIO fileIO = fileStoreTable.fileIO();

        List<MigrateTask> tasks = new ArrayList<>();
        Map<Path, Path> rollBack = new ConcurrentHashMap<>();
        if (fileStoreTable.partitionKeys().isEmpty()) {
            tasks.add(importUnPartitionedTableTask(fileIO, fileStoreTable, rollBack));
        } else {
            tasks.addAll(
                    importPartitionedTableTask(fileIO, partitionNames, fileStoreTable, rollBack));
        }

        List<CommitMessage> commitMessages = new ArrayList<>();

        List<Future<CommitMessage>> futures =
                tasks.stream().map(COMMON_IO_FORK_JOIN_POOL::submit).collect(Collectors.toList());

        try {
            for (Future<CommitMessage> future : futures) {
                commitMessages.add(future.get());
            }
        } catch (Exception e) {
            futures.forEach(f -> f.cancel(true));
            for (Future<?> future : futures) {
                // wait all task cancelled or finished
                while (!future.isDone()) {
                    //noinspection BusyWait
                    Thread.sleep(100);
                }
            }
            // roll back all renamed path
            for (Map.Entry<Path, Path> entry : rollBack.entrySet()) {
                Path newPath = entry.getKey();
                Path origin = entry.getValue();
                if (fileIO.exists(newPath)) {
                    fileIO.rename(newPath, origin);
                }
            }
            throw new RuntimeException("Migrating failed because exception happens", e);
        }
        return commitMessages;
    }

    private List<MigrateTask> importPartitionedTableTask(
            FileIO fileIO,
            Set<String> partitionNames,
            FileStoreTable paimonTable,
            Map<Path, Path> rollback) {
        List<MigrateTask> migrateTasks = new ArrayList<>();
        List<BinaryWriter.ValueSetter> valueSetters = new ArrayList<>();

        RowType partitionRowType =
                paimonTable.schema().projectedLogicalRowType(paimonTable.schema().partitionKeys());

        partitionRowType
                .getFieldTypes()
                .forEach(type -> valueSetters.add(BinaryWriter.createValueSetter(type)));

        for (String partitionName : partitionNames) {
            Path path = new Path(fileStoreTable.location(), partitionName);
            Map<String, String> partitionValues = new HashMap<>();
            for (String s : partitionName.split(",")) {
                String[] split = s.split("=");
                partitionValues.put(split[0], split[1]);
            }
            BinaryRow partitionRow =
                    FileMetaUtils.writePartitionValue(
                            partitionRowType, partitionValues, valueSetters);
            migrateTasks.add(
                    new MigrateTask(
                            fileIO,
                            "parquet",
                            path.toString(),
                            paimonTable,
                            partitionRow,
                            path,
                            rollback));
        }
        return migrateTasks;
    }

    public MigrateTask importUnPartitionedTableTask(
            FileIO fileIO, FileStoreTable paimonTable, Map<Path, Path> rollback) {
        return new MigrateTask(
                fileIO,
                "parquet",
                paimonTable.location().toString(),
                paimonTable,
                BinaryRow.EMPTY_ROW,
                paimonTable.location(),
                rollback);
    }

    /** 1. */
    public static class MigrateTask implements Callable<CommitMessage> {

        private final FileIO fileIO;
        private final String format;
        private final String location;
        private final FileStoreTable paimonTable;
        private final BinaryRow partitionRow;
        private final Path newDir;
        private final Map<Path, Path> rollback;

        public MigrateTask(
                FileIO fileIO,
                String format,
                String location,
                FileStoreTable paimonTable,
                BinaryRow partitionRow,
                Path newDir,
                Map<Path, Path> rollback) {
            this.fileIO = fileIO;
            this.format = format;
            this.location = location;
            this.paimonTable = paimonTable;
            this.partitionRow = partitionRow;
            this.newDir = newDir;
            this.rollback = rollback;
        }

        @Override
        public CommitMessage call() throws Exception {
            if (!fileIO.exists(newDir)) {
                fileIO.mkdirs(newDir);
            }
            List<DataFileMeta> fileMetas =
                    FileMetaUtils.construct(
                            fileIO,
                            format,
                            location,
                            paimonTable,
                            HIDDEN_PATH_FILTER,
                            newDir,
                            rollback);
            return FileMetaUtils.commitFile(partitionRow, fileMetas);
        }
    }
}
