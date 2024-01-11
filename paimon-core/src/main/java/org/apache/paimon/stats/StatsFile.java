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

package org.apache.paimon.stats;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.utils.PathFactory;

import java.io.IOException;
import java.io.UncheckedIOException;

/** Stats file contains stats. */
public class StatsFile {

    private final FileIO fileIO;
    private final PathFactory pathFactory;

    public StatsFile(FileIO fileIO, PathFactory pathFactory) {
        this.fileIO = fileIO;
        this.pathFactory = pathFactory;
    }

    /**
     * Read stats from stat file name.
     *
     * @return stats
     */
    public Stats read(String fileName) {
        return Stats.fromPath(fileIO, pathFactory.toPath(fileName));
    }

    /**
     * Write stats to stats file.
     *
     * @return stats file name
     */
    public String write(Stats stats) {
        Path path = pathFactory.newPath();

        try {
            fileIO.writeFileUtf8(path, stats.toJson());
        } catch (IOException e) {
            throw new RuntimeException("Failed to write stats file: " + path, e);
        }
        return path.getName();
    }

    public void delete(String fileName) {
        fileIO.deleteQuietly(pathFactory.toPath(fileName));
    }

    public boolean exists(String fileName) {
        try {
            return fileIO.exists(pathFactory.toPath(fileName));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
