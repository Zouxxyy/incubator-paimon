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
import org.apache.paimon.catalog.PrimaryKeyTableTestBase;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexMaintainer;
import org.apache.paimon.types.RowKind;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

/** Test for {@link DeleteMapIndexMaintainer}. */
public class DeleteMapIndexMaintainerTest extends PrimaryKeyTableTestBase {
    private IndexFileHandler fileHandler;

    @BeforeEach
    public void beforeEach() throws Exception {
        fileHandler = table.store().newIndexFileHandler();
    }

    @Test
    public void test0() {
        IndexMaintainer<KeyValue, DeleteIndex> deleteMapIndexWriter =
                new DeleteMapIndexMaintainer(fileHandler, null, BinaryRow.EMPTY_ROW, 0);

        deleteMapIndexWriter.notifyNewRecord(kv(1, 1, 1, "f1"));
        deleteMapIndexWriter.notifyNewRecord(kv(1, 1, 1, "f2"));

        List<IndexFileMeta> fileMetas = deleteMapIndexWriter.prepareCommit();

        Map<String, DeleteIndex> deleteMap = fileHandler.readAllDeleteIndex(fileMetas.get(0));
        Assertions.assertTrue(deleteMap.get("f1").isDeleted(1));
        Assertions.assertFalse(deleteMap.get("f1").isDeleted(2));
        Assertions.assertTrue(deleteMap.get("f1").isDeleted(1));
        Assertions.assertFalse(deleteMap.get("f1").isDeleted(2));
    }

    private KeyValue kv(int key, int value, long position, String fileName) {
        return new KeyValue()
                .replace(GenericRow.of(key), RowKind.INSERT, GenericRow.of(key, value))
                .setPosition(position)
                .setFileName(fileName);
    }
}
