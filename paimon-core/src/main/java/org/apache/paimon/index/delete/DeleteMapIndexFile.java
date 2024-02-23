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
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.PathFactory;

import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.CRC32;

/** x. */
public class DeleteMapIndexFile {

    public static final String DELETE_MAP_INDEX = "DELETE_MAP";
    public static final byte VERSION_ID_V1 = 1;
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

    public Map<String, DeleteIndex> readAllDeleteIndex(
            String fileName, Map<String, Pair<Integer, Integer>> deleteIndexRanges) {
        Map<String, DeleteIndex> deleteIndexMap = new HashMap<>();
        try (SeekableInputStream inputStream =
                fileIO.newInputStream(pathFactory.toPath(fileName))) {
            checkVersion(inputStream);
            for (Map.Entry<String, Pair<Integer, Integer>> entry : deleteIndexRanges.entrySet()) {
                deleteIndexMap.put(entry.getKey(), seekDeleteIndex(inputStream, entry.getValue()));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return deleteIndexMap;
    }

    public DeleteIndex readDeleteIndex(String fileName, Pair<Integer, Integer> deleteIndexRange) {
        try (SeekableInputStream inputStream =
                fileIO.newInputStream(pathFactory.toPath(fileName))) {
            checkVersion(inputStream);
            return seekDeleteIndex(inputStream, deleteIndexRange);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public Pair<String, Map<String, Pair<Integer, Integer>>> write(Map<String, DeleteIndex> input) {
        int size = input.size();
        HashMap<String, Pair<Integer, Integer>> deleteIndexRanges = new HashMap<>(size);
        Path path = pathFactory.newPath();
        try (DataOutputStream dataOutputStream =
                new DataOutputStream(fileIO.newOutputStream(path, true))) {
            dataOutputStream.writeByte(VERSION_ID_V1);
            for (Map.Entry<String, DeleteIndex> entry : input.entrySet()) {
                String key = entry.getKey();
                byte[] valueBytes = entry.getValue().serializeToBytes();
                deleteIndexRanges.put(key, Pair.of(dataOutputStream.size(), valueBytes.length));
                dataOutputStream.writeInt(valueBytes.length);
                dataOutputStream.write(valueBytes);
                dataOutputStream.writeInt(calculateChecksum(valueBytes));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return Pair.of(path.getName(), deleteIndexRanges);
    }

    public void delete(String fileName) {
        fileIO.deleteQuietly(pathFactory.toPath(fileName));
    }

    // -------------------------------------------------------------------------
    //  Internal methods
    // -------------------------------------------------------------------------

    private void checkVersion(SeekableInputStream in) throws IOException {
        int version = in.read();
        if (version != VERSION_ID_V1) {
            throw new RuntimeException(
                    "Version not match, actual size: "
                            + version
                            + ", expert size: "
                            + VERSION_ID_V1);
        }
    }

    private int readInt(SeekableInputStream in) throws IOException {
        int ch1 = in.read();
        int ch2 = in.read();
        int ch3 = in.read();
        int ch4 = in.read();
        if ((ch1 | ch2 | ch3 | ch4) < 0) {
            throw new EOFException();
        }
        return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4));
    }

    private DeleteIndex seekDeleteIndex(
            SeekableInputStream inputStream, Pair<Integer, Integer> deleteIndexRange) {
        try {
            Integer start = deleteIndexRange.getLeft();
            Integer size = deleteIndexRange.getRight();

            // check size
            inputStream.seek(start);
            int actualSize = readInt(inputStream);
            if (actualSize != size) {
                throw new RuntimeException(
                        "Size not match, actual size: " + actualSize + ", expert size: " + size);
            }

            // read deleteIndex bytes
            inputStream.seek(start + 4);
            byte[] bytes = new byte[deleteIndexRange.getRight()];
            inputStream.read(bytes);

            // check checksum
            inputStream.seek(start + 4 + size);
            int checkSum = calculateChecksum(bytes);
            int actualCheckSum = readInt(inputStream);
            if (actualCheckSum != checkSum) {
                throw new RuntimeException(
                        "Checksum not match, actual checksum: "
                                + actualCheckSum
                                + ", expected checksum: "
                                + checkSum);
            }

            return new BitmapDeleteIndex().deserializeFromBytes(bytes);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private int calculateChecksum(byte[] bytes) {
        CRC32 crc = new CRC32();
        crc.update(bytes);
        return (int) crc.getValue();
    }
}
