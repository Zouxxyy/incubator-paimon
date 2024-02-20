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

import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.utils.PathFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/** Test for {@link DeleteMapIndexFile}. */
public class DeleteMapIndexFileTest {
    @TempDir java.nio.file.Path tempPath;

    @Test
    public void test0() {
        Path dir = new Path(tempPath.toUri());
        PathFactory pathFactory =
                new PathFactory() {
                    @Override
                    public Path newPath() {
                        return new Path(dir, UUID.randomUUID().toString());
                    }

                    @Override
                    public Path toPath(String fileName) {
                        return new Path(dir, fileName);
                    }
                };

        DeleteMapIndexFile deleteMapIndexFile =
                new DeleteMapIndexFile(LocalFileIO.create(), pathFactory);

        // write
        HashMap<String, DeleteIndex> deleteMap = new HashMap<>();
        BitmapDeleteIndex index1 = new BitmapDeleteIndex();
        index1.delete(1);
        deleteMap.put("file1.parquet", index1);

        BitmapDeleteIndex index2 = new BitmapDeleteIndex();
        index2.delete(2);
        index2.delete(3);
        deleteMap.put("file2.parquet", index2);

        BitmapDeleteIndex index3 = new BitmapDeleteIndex();
        index3.delete(3);
        deleteMap.put("file33.parquet", index3);

        String deleteMapIndexFileName = deleteMapIndexFile.write(deleteMap);

        // read
        Map<String, long[]> deleteIndexBytesOffsets =
                deleteMapIndexFile.readDeleteIndexBytesOffsets(deleteMapIndexFileName);
        Map<String, DeleteIndex> actualDeleteMap =
                deleteMapIndexFile.readAllDeleteIndex(
                        deleteMapIndexFileName, deleteIndexBytesOffsets);
        Assertions.assertTrue(actualDeleteMap.get("file1.parquet").isDeleted(1));
        Assertions.assertFalse(actualDeleteMap.get("file1.parquet").isDeleted(2));
        Assertions.assertTrue(actualDeleteMap.get("file2.parquet").isDeleted(2));
        Assertions.assertTrue(actualDeleteMap.get("file2.parquet").isDeleted(3));
        Assertions.assertTrue(actualDeleteMap.get("file33.parquet").isDeleted(3));

        DeleteIndex file1DeleteIndex =
                deleteMapIndexFile.readDeleteIndex(
                        deleteMapIndexFileName, deleteIndexBytesOffsets.get("file1.parquet"));
        Assertions.assertTrue(file1DeleteIndex.isDeleted(1));
        Assertions.assertFalse(file1DeleteIndex.isDeleted(2));
    }
}
