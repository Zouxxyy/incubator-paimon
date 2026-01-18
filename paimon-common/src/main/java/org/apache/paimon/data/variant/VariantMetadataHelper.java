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

import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Helper class for marking and identifying variant-originated RowType following Spark's approach.
 * Uses description field in DataField to encode variant metadata.
 *
 * <p>Description format: __VARIANT_METADATA:&lt;path&gt;;&lt;failOnError&gt;;&lt;timeZoneId&gt;
 *
 * <p>Example: __VARIANT_METADATA:$.a.b;true;UTC
 */
public class VariantMetadataHelper {

    public static final String METADATA_KEY = "__VARIANT_METADATA";
    public static final String DELIMITER = ";";
    public static final String KEY_VALUE_SEPARATOR = ":";

    /**
     * Create variant metadata description string.
     *
     * @param path the extraction path (e.g., "$.a.b")
     * @param failOnError whether to fail on error
     * @param timeZoneId the time zone id (e.g., "UTC")
     * @return encoded metadata string
     */
    public static String createVariantMetadata(
            String path, boolean failOnError, String timeZoneId) {
        return METADATA_KEY
                + KEY_VALUE_SEPARATOR
                + path
                + DELIMITER
                + failOnError
                + DELIMITER
                + timeZoneId;
    }

    /**
     * Create variant metadata description string with default values.
     *
     * @param path the extraction path (e.g., "$.a.b")
     * @return encoded metadata string with default failOnError=false and timeZoneId=UTC
     */
    public static String createVariantMetadata(String path) {
        return createVariantMetadata(path, false, "UTC");
    }

    /**
     * Check if a RowType is originated from a variant by checking if all fields contain variant
     * metadata in their descriptions.
     *
     * @param rowType the RowType to check
     * @return true if all fields have variant metadata, false otherwise
     */
    public static boolean isVariantRowType(RowType rowType) {
        if (rowType == null || rowType.getFields().isEmpty()) {
            return false;
        }
        for (DataField field : rowType.getFields()) {
            if (!isVariantField(field)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Check if a DataField contains variant metadata in its description.
     *
     * @param field the DataField to check
     * @return true if the field has variant metadata, false otherwise
     */
    public static boolean isVariantField(DataField field) {
        String description = field.description();
        return description != null && description.startsWith(METADATA_KEY);
    }

    /**
     * Extract the path from variant metadata description.
     *
     * @param description the description string
     * @return the path, or null if not valid variant metadata
     */
    @Nullable
    public static String extractPath(String description) {
        if (description == null || !description.startsWith(METADATA_KEY)) {
            return null;
        }
        String content =
                description.substring(METADATA_KEY.length() + KEY_VALUE_SEPARATOR.length());
        String[] parts = content.split(DELIMITER);
        return parts.length > 0 ? parts[0] : null;
    }

    /**
     * Extract the failOnError flag from variant metadata description.
     *
     * @param description the description string
     * @return the failOnError flag, or false if not valid variant metadata
     */
    public static boolean extractFailOnError(String description) {
        if (description == null || !description.startsWith(METADATA_KEY)) {
            return false;
        }
        String content =
                description.substring(METADATA_KEY.length() + KEY_VALUE_SEPARATOR.length());
        String[] parts = content.split(DELIMITER);
        return parts.length > 1 && Boolean.parseBoolean(parts[1]);
    }

    /**
     * Extract the time zone id from variant metadata description.
     *
     * @param description the description string
     * @return the time zone id, or "UTC" if not valid variant metadata
     */
    public static String extractTimeZoneId(String description) {
        if (description == null || !description.startsWith(METADATA_KEY)) {
            return "UTC";
        }
        String content =
                description.substring(METADATA_KEY.length() + KEY_VALUE_SEPARATOR.length());
        String[] parts = content.split(DELIMITER);
        return parts.length > 2 ? parts[2] : "UTC";
    }

    /** Builder for creating variant row types with metadata. */
    public static class VariantRowTypeBuilder {

        private final List<DataField> fields = new ArrayList<>();

        private final boolean isNullable;
        private final AtomicInteger fieldId;

        private VariantRowTypeBuilder(boolean isNullable, AtomicInteger fieldId) {
            this.isNullable = isNullable;
            this.fieldId = fieldId;
        }

        public VariantRowTypeBuilder field(
                DataType type, String path, boolean failOnError, String timeZoneId) {
            int id = fieldId.incrementAndGet();
            fields.add(
                    new DataField(
                            id,
                            String.valueOf(id),
                            type,
                            VariantMetadataHelper.createVariantMetadata(
                                    path, failOnError, timeZoneId)));
            return this;
        }

        public static VariantRowTypeBuilder builder(boolean isNullable) {
            return new VariantRowTypeBuilder(isNullable, new AtomicInteger(-1));
        }

        public RowType build() {
            return new RowType(isNullable, fields);
        }
    }

    /**
     * Clip the variant schema to read with variant access fields from RowType with metadata.
     *
     * @param shreddingSchema the shredding schema
     * @param variantRowType the RowType with variant metadata
     * @return clipped variant schema
     */
    public static RowType clipVariantSchema(RowType shreddingSchema, RowType variantRowType) {
        if (!shreddingSchema.containsField(PaimonShreddingUtils.TYPED_VALUE_FIELD_NAME)) {
            return shreddingSchema;
        }

        boolean canClip = true;
        java.util.Set<String> fieldsToRead = new java.util.HashSet<>();
        for (DataField field : variantRowType.getFields()) {
            String path = extractPath(field.description());
            VariantPathSegment[] pathSegments = VariantPathSegment.parse(path);
            if (pathSegments.length < 1) {
                canClip = false;
                break;
            }

            // todo: support nested column pruning
            VariantPathSegment pathSegment = pathSegments[0];
            if (pathSegment instanceof VariantPathSegment.ObjectExtraction) {
                fieldsToRead.add(((VariantPathSegment.ObjectExtraction) pathSegment).getKey());
            } else {
                canClip = false;
                break;
            }
        }

        if (!canClip) {
            return shreddingSchema;
        }

        java.util.List<DataField> typedFieldsToRead = new java.util.ArrayList<>();
        DataField typedValue =
                shreddingSchema.getField(PaimonShreddingUtils.TYPED_VALUE_FIELD_NAME);
        for (DataField field : ((RowType) typedValue.type()).getFields()) {
            if (fieldsToRead.contains(field.name())) {
                typedFieldsToRead.add(field);
                fieldsToRead.remove(field.name());
            }
        }

        java.util.List<DataField> shreddingSchemaFields = new java.util.ArrayList<>();
        shreddingSchemaFields.add(
                shreddingSchema.getField(PaimonShreddingUtils.METADATA_FIELD_NAME));
        // If there are fields to read not in the `typed_value`, add the `value` field.
        if (!fieldsToRead.isEmpty()) {
            shreddingSchemaFields.add(
                    shreddingSchema.getField(PaimonShreddingUtils.VARIANT_VALUE_FIELD_NAME));
        }
        if (!typedFieldsToRead.isEmpty()) {
            shreddingSchemaFields.add(typedValue.newType(new RowType(typedFieldsToRead)));
        }
        return new RowType(shreddingSchemaFields);
    }
}
