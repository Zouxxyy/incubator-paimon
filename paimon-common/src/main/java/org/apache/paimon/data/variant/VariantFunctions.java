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

package org.apache.paimon.data.variant;

import org.apache.paimon.data.variant.VariantUtil.Type;

/** 1. */
public class VariantFunctions {

    public static Object variantGet(Variant v, String path) {
        PathSegment[] parsedPath = PathSegment.parse(path);
        for (PathSegment pathSegment : parsedPath) {
            if (pathSegment.isKey() && v.getType() == Type.OBJECT) {
                v = v.getFieldByKey(pathSegment.getKey());
            } else if (pathSegment.isIndex() && v.getType() == Type.ARRAY) {
                v = v.getElementAtIndex(pathSegment.getIndex());
            } else {
                return null;
            }
        }

        switch (v.getType()) {
            case OBJECT:
            case ARRAY:
                return v.toJson();
            case STRING:
                return v.getString();
            case LONG:
                return v.getLong();
            case DOUBLE:
                return v.getDouble();
            case DECIMAL:
                return v.getDecimal();
            case BOOLEAN:
                return v.getBoolean();
            case NULL:
                return null;
            default:
                throw new IllegalArgumentException("Unsupported type: " + v.getType());
        }
    }
}
