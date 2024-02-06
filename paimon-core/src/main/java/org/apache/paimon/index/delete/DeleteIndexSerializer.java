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

import org.apache.paimon.utils.JsonDeserializer;
import org.apache.paimon.utils.JsonSerializer;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;

/** x. */
public class DeleteIndexSerializer
        implements JsonSerializer<DeleteIndex>, JsonDeserializer<DeleteIndex> {

    public static final DeleteIndexSerializer INSTANCE = new DeleteIndexSerializer();

    @Override
    public void serialize(DeleteIndex deleteIndex, JsonGenerator generator) throws IOException {
        generator.writeBinary(deleteIndex.serializeToBytes());
    }

    @Override
    public DeleteIndex deserialize(JsonNode node) {
        byte[] bytes = java.util.Base64.getDecoder().decode(node.asText());
        return new BitmapDeleteIndex().deserializeFromBytes(bytes);
    }
}
