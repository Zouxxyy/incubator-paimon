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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.annotation.Public;
import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataOutputView;

import java.io.IOException;
import java.util.Objects;

/**
 * A data file from the table which can be read directly without merging.
 *
 * @since 0.6.0
 */
@Public
public class RawFile {

    private final String path;
    private final long offset;
    private final long length;
    private final String format;
    private final long schemaId;

    private final long rowCount;

    private final DeleteFile deleteFile;

    public RawFile(
            String path, long offset, long length, String format, long schemaId, long rowCount) {
        this(path, offset, length, format, schemaId, rowCount, DeleteFile.EMPTY_DELETE_FILE);
    }

    public RawFile(
            String path,
            long offset,
            long length,
            String format,
            long schemaId,
            long rowCount,
            DeleteFile deleteFile) {
        this.path = path;
        this.offset = offset;
        this.length = length;
        this.format = format;
        this.schemaId = schemaId;
        this.rowCount = rowCount;
        this.deleteFile = deleteFile;
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

    /**
     * Format of the file, which is a lower-cased string. See {@link CoreOptions.FileFormatType} for
     * all possible types.
     */
    public String format() {
        return format;
    }

    /** Schema id of the file. */
    public long schemaId() {
        return schemaId;
    }

    /** row count of the file. */
    public long rowCount() {
        return rowCount;
    }

    /** @since 0.8.0 */
    public DeleteFile deleteFile() {
        return deleteFile;
    }

    public void serialize(DataOutputView out) throws IOException {
        out.writeUTF(path);
        out.writeLong(offset);
        out.writeLong(length);
        out.writeUTF(format);
        out.writeLong(schemaId);
        out.writeLong(rowCount);
        deleteFile.serialize(out);
    }

    public static RawFile deserialize(DataInputView in) throws IOException {
        String path = in.readUTF();
        long offset = in.readLong();
        long length = in.readLong();
        String format = in.readUTF();
        long schemaId = in.readLong();
        long rowCount = in.readLong();
        DeleteFile deleteFile = DeleteFile.deserialize(in);
        return new RawFile(path, offset, length, format, schemaId, rowCount, deleteFile);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof RawFile)) {
            return false;
        }

        RawFile other = (RawFile) o;
        return Objects.equals(path, other.path)
                && offset == other.offset
                && length == other.length
                && Objects.equals(format, other.format)
                && schemaId == other.schemaId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, offset, length, format, schemaId);
    }

    @Override
    public String toString() {
        return String.format(
                "{path = %s, offset = %d, length = %d, format = %s, schemaId = %d}",
                path, offset, length, format, schemaId);
    }

    /** 1. */
    public static class DeleteFile {

        public static final DeleteFile EMPTY_DELETE_FILE = new DeleteFile("", 0, 0);
        private final String path;
        private final int offset;
        private final int length;

        public DeleteFile(String path, int offset, int length) {
            this.path = path;
            this.offset = offset;
            this.length = length;
        }

        public String path() {
            return path;
        }

        public int offset() {
            return offset;
        }

        public int length() {
            return length;
        }

        public boolean nonEmpty() {
            return !this.equals(EMPTY_DELETE_FILE);
        }

        public void serialize(DataOutputView out) throws IOException {
            out.writeUTF(path);
            if (nonEmpty()) {
                out.writeInt(offset);
                out.writeInt(length);
            }
        }

        public static DeleteFile deserialize(DataInputView in) throws IOException {
            String path = in.readUTF();
            if (path.isEmpty()) {
                return EMPTY_DELETE_FILE;
            } else {
                int offset = in.readInt();
                int length = in.readInt();
                return new DeleteFile(path, offset, length);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            DeleteFile that = (DeleteFile) o;
            return offset == that.offset
                    && length == that.length
                    && Objects.equals(path, that.path);
        }

        @Override
        public int hashCode() {
            return Objects.hash(path, offset, length);
        }

        @Override
        public String toString() {
            if (nonEmpty()) {
                return "DeleteFile{"
                        + "path='"
                        + path
                        + '\''
                        + ", offset="
                        + offset
                        + ", length="
                        + length
                        + '}';
            } else {
                return "DeleteFile{empty}";
            }
        }
    }
}
