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

import org.apache.paimon.annotation.Public;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataOutputView;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Deletion file for data file, the first 4 bytes are length, should, the following is the bitmap
 * content.
 *
 * <ul>
 *   <li>The first 4 bytes are length, should equal to {@link #length()}.
 *   <li>Next 4 bytes are the magic number, should be equal to 1581511376.
 *   <li>The remaining content should be a RoaringBitmap.
 * </ul>
 */
@Public
public class DeletionFile implements Serializable {

    private static final long serialVersionUID = 2L;

    private final String path;
    private final long offset;
    private final long length;
    @Nullable private final Long cardinality;
    @Nullable private final Long first;
    @Nullable private final Long last;

    public DeletionFile(
            String path,
            long offset,
            long length,
            @Nullable Long cardinality,
            @Nullable Long first,
            @Nullable Long last) {
        this.path = path;
        this.offset = offset;
        this.length = length;
        this.cardinality = cardinality;
        this.first = first;
        this.last = last;
    }

    /** Path of the file. */
    public String path() {
        return path;
    }

    /** Starting offset of data in the file. */
    public long offset() {
        return offset;
    }

    /** Length of data in the file. */
    public long length() {
        return length;
    }

    @Nullable
    public Long cardinality() {
        return cardinality;
    }

    @Nullable
    public Long first() {
        return first;
    }

    @Nullable
    public Long last() {
        return last;
    }

    public static void serialize(DataOutputView out, @Nullable DeletionFile file)
            throws IOException {
        if (file == null) {
            out.write(0);
        } else {
            out.write(1);
            out.writeUTF(file.path);
            out.writeLong(file.offset);
            out.writeLong(file.length);
            out.writeLong(file.cardinality == null ? -1 : file.cardinality);
            out.writeLong(file.first == null ? -1 : file.first);
            out.writeLong(file.last == null ? -1 : file.last);
        }
    }

    public static void serializeList(DataOutputView out, @Nullable List<DeletionFile> files)
            throws IOException {
        if (files == null) {
            out.write(0);
        } else {
            out.write(1);
            out.writeInt(files.size());
            for (DeletionFile file : files) {
                serialize(out, file);
            }
        }
    }

    @Nullable
    public static DeletionFile deserialize(DataInputView in) throws IOException {
        if (in.readByte() == 0) {
            return null;
        }

        String path = in.readUTF();
        long offset = in.readLong();
        long length = in.readLong();
        long cardinality = in.readLong();
        long first = in.readLong();
        long last = in.readLong();
        return new DeletionFile(
                path,
                offset,
                length,
                cardinality == -1 ? null : cardinality,
                first == -1 ? null : first,
                last == -1 ? null : last);
    }

    @Nullable
    public static List<DeletionFile> deserializeList(DataInputView in) throws IOException {
        List<DeletionFile> files = null;
        if (in.readByte() == 1) {
            int size = in.readInt();
            files = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                files.add(DeletionFile.deserialize(in));
            }
        }
        return files;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DeletionFile that = (DeletionFile) o;
        return offset == that.offset
                && length == that.length
                && Objects.equals(path, that.path)
                && Objects.equals(cardinality, that.cardinality)
                && Objects.equals(first, that.first)
                && Objects.equals(last, that.last);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, offset, length, cardinality, first, last);
    }

    @Override
    public String toString() {
        return "DeletionFile{"
                + "path='"
                + path
                + '\''
                + ", offset="
                + offset
                + ", length="
                + length
                + ", cardinality="
                + cardinality
                + ", first="
                + first
                + ", last="
                + last
                + '}';
    }

    static Factory emptyFactory() {
        return fileName -> Optional.empty();
    }

    public static Factory factory(
            List<DataFileMeta> files, @Nullable List<DeletionFile> deletionFiles) {
        if (deletionFiles == null) {
            return emptyFactory();
        }
        Map<String, DeletionFile> fileToDeletion = new HashMap<>();
        for (int i = 0; i < files.size(); i++) {
            DeletionFile deletionFile = deletionFiles.get(i);
            if (deletionFile != null) {
                fileToDeletion.put(files.get(i).fileName(), deletionFile);
            }
        }
        return fileName -> {
            DeletionFile deletionFile = fileToDeletion.get(fileName);
            return Optional.ofNullable(deletionFile);
        };
    }

    /** Interface to create {@link DeletionFile}. */
    public interface Factory {
        Optional<DeletionFile> create(String fileName) throws IOException;
    }
}
