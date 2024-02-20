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
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.utils.PathFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;

/** x. */
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

    public Map<String, long[]> readDeleteIndexBytesOffsets(String fileName) {
        try (DataInputStream dataInputStream =
                new DataInputStream(fileIO.newInputStream(pathFactory.toPath(fileName)))) {
            int size = dataInputStream.readInt();
            int maxKeyLength = dataInputStream.readInt();
            Map<String, long[]> map = new HashMap<>();
            for (int i = 0; i < size; i++) {
                byte[] bytes = new byte[maxKeyLength];
                dataInputStream.read(bytes);
                String key = new String(bytes).trim();
                long start = dataInputStream.readLong();
                long length = dataInputStream.readLong();
                map.put(key, new long[] {start, length});
            }
            return map;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public Map<String, DeleteIndex> readAllDeleteIndex(
            String fileName, Map<String, long[]> deleteIndexBytesOffsets) {
        Map<String, DeleteIndex> deleteIndexMap = new HashMap<>();
        try (SeekableInputStream inputStream =
                fileIO.newInputStream(pathFactory.toPath(fileName))) {
            for (Map.Entry<String, long[]> entry : deleteIndexBytesOffsets.entrySet()) {
                long[] offset = entry.getValue();
                inputStream.seek(offset[0]);
                byte[] bytes = new byte[(int) offset[1]];
                inputStream.read(bytes);
                deleteIndexMap.put(
                        entry.getKey(), new BitmapDeleteIndex().deserializeFromBytes(bytes));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return deleteIndexMap;
    }

    public DeleteIndex readDeleteIndex(String fileName, long[] deleteIndexBytesOffset) {
        try (SeekableInputStream inputStream =
                fileIO.newInputStream(pathFactory.toPath(fileName))) {
            inputStream.seek(deleteIndexBytesOffset[0]);
            byte[] bytes = new byte[(int) deleteIndexBytesOffset[1]];
            inputStream.read(bytes);
            return new BitmapDeleteIndex().deserializeFromBytes(bytes);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public String write(Map<String, DeleteIndex> input) {
        int size = input.size();
        int maxKeyLength = input.keySet().stream().mapToInt(String::length).max().orElse(0);

        byte[][] keyBytesArray = new byte[size][];
        long[] startArray = new long[size];
        long[] lengthArray = new long[size];
        byte[][] valueBytesArray = new byte[size][];
        int i = 0;
        long start = 8 + (long) size * (maxKeyLength + 16);
        for (Map.Entry<String, DeleteIndex> entry : input.entrySet()) {
            String key = entry.getKey();
            byte[] valueBytes = entry.getValue().serializeToBytes();
            keyBytesArray[i] = String.format("%" + maxKeyLength + "s", key).getBytes();
            startArray[i] = start;
            lengthArray[i] = valueBytes.length;
            valueBytesArray[i] = valueBytes;
            start += valueBytes.length;
            i++;
        }

        Path path = pathFactory.newPath();
        // File structure:
        //  mapSize (int), maxKeyLength (int)
        //  Array of <padded keyBytes (maxKeyLength), start (long), length (long)>
        //  Array of <valueBytes>
        try (DataOutputStream dataOutputStream =
                new DataOutputStream(fileIO.newOutputStream(path, true))) {
            dataOutputStream.writeInt(size);
            dataOutputStream.writeInt(maxKeyLength);

            for (int j = 0; j < size; j++) {
                dataOutputStream.write(keyBytesArray[j]);
                dataOutputStream.writeLong(startArray[j]);
                dataOutputStream.writeLong(lengthArray[j]);
            }

            for (int j = 0; j < size; j++) {
                dataOutputStream.write(valueBytesArray[j]);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return path.getName();
    }

    public void delete(String fileName) {
        fileIO.deleteQuietly(pathFactory.toPath(fileName));
    }
}
