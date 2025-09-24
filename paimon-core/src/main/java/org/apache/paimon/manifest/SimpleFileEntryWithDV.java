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

package org.apache.paimon.manifest;

import org.apache.paimon.data.BinaryRow;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;

/** A {@link FileEntry} contains simpleFileEntry and dv file name. */
public class SimpleFileEntryWithDV extends SimpleFileEntry {

    private final String dvFileName;

    private SimpleFileEntryWithDV(
            FileKind kind,
            BinaryRow partition,
            int bucket,
            int totalBuckets,
            int level,
            String fileName,
            List<String> extraFiles,
            @Nullable byte[] embeddedIndex,
            BinaryRow minKey,
            BinaryRow maxKey,
            @Nullable String externalPath,
            @Nullable String dvFileName) {
        super(
                kind,
                partition,
                bucket,
                totalBuckets,
                level,
                fileName,
                extraFiles,
                embeddedIndex,
                minKey,
                maxKey,
                externalPath);
        this.dvFileName = dvFileName;
    }

    public Identifier identifier() {
        return new IdentifierWithDv(
                partition(),
                bucket(),
                level(),
                fileName(),
                extraFiles(),
                embeddedIndex(),
                externalPath(),
                dvFileName);
    }

    public static SimpleFileEntryWithDV from(
            SimpleFileEntry fileEntry, @Nullable String dvFileName) {
        return new SimpleFileEntryWithDV(
                fileEntry.kind(),
                fileEntry.partition(),
                fileEntry.bucket(),
                fileEntry.totalBuckets(),
                fileEntry.level(),
                fileEntry.fileName(),
                fileEntry.extraFiles(),
                fileEntry.embeddedIndex(),
                fileEntry.minKey(),
                fileEntry.maxKey(),
                fileEntry.externalPath(),
                dvFileName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        SimpleFileEntryWithDV that = (SimpleFileEntryWithDV) o;
        return Objects.equals(dvFileName, that.dvFileName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), dvFileName);
    }

    /**
     * The same {@link Identifier} indicates that the {@link ManifestEntry} refers to the same data
     * file.
     */
    static class IdentifierWithDv extends Identifier {

        private final String dvFileName;

        public IdentifierWithDv(
                BinaryRow partition,
                int bucket,
                int level,
                String fileName,
                List<String> extraFiles,
                @Nullable byte[] embeddedIndex,
                @Nullable String externalPath,
                String dvFileName) {
            super(partition, bucket, level, fileName, extraFiles, embeddedIndex, externalPath);
            this.dvFileName = dvFileName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            if (!super.equals(o)) {
                return false;
            }
            IdentifierWithDv that = (IdentifierWithDv) o;
            return Objects.equals(dvFileName, that.dvFileName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), dvFileName);
        }
    }
}
