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

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.PathFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/** todo: add an abstract file. */
public class DeleteMapIndexFile {

    public static final String DELETE_MAP_INDEX = "DELETE_MAP";

    private final FileIO fileIO;
    private final PathFactory pathFactory;

    public DeleteMapIndexFile(FileIO fileIO, PathFactory pathFactory) {
        this.fileIO = fileIO;
        this.pathFactory = pathFactory;
    }

    public long fileSize(String fileName) {
        try {
            return fileIO.getFileSize(pathFactory.toPath(fileName));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public Map<String, DeleteIndex> read(String fileName) throws IOException {
        String json = fileIO.readFileUtf8(pathFactory.toPath(fileName));
        LinkedHashMap<String, String> map =
                (LinkedHashMap<String, String>) JsonSerdeUtil.fromJson(json, Map.class);
        Map<String, DeleteIndex> deleteIndexMap = new HashMap<>();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            byte[] bytes = java.util.Base64.getDecoder().decode(entry.getValue());
            deleteIndexMap.put(entry.getKey(), new BitmapDeleteIndex().deserializeFromBytes(bytes));
        }
        return deleteIndexMap;
    }

    public Optional<DeleteIndex> read(String fileName, String dataFileName) throws IOException {
        // todo: support only read specific file's bitmap
        return Optional.ofNullable(read(fileName).get(dataFileName));
    }

    public String write(Map<String, DeleteIndex> input) throws IOException {
        Path path = pathFactory.newPath();
        try {
            String json = JsonSerdeUtil.toJson(input);
            fileIO.writeFileUtf8(path, json);
        } catch (IOException e) {
            throw new RuntimeException("Failed to xxx: " + path, e);
        }
        return path.getName();
    }

    public void delete(String fileName) {
        fileIO.deleteQuietly(pathFactory.toPath(fileName));
    }
}
