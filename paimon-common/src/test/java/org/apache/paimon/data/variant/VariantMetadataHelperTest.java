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
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link VariantMetadataHelper}. */
public class VariantMetadataHelperTest {

    @Test
    public void testCreateVariantMetadata() {
        String metadata = VariantMetadataHelper.createVariantMetadata("$.a.b", true, "UTC");
        assertThat(metadata).isEqualTo("__VARIANT_METADATA:$.a.b;true;UTC");
    }

    @Test
    public void testCreateVariantMetadataWithDefaults() {
        String metadata = VariantMetadataHelper.createVariantMetadata("$.field");
        assertThat(metadata).isEqualTo("__VARIANT_METADATA:$.field;false;UTC");
    }

    @Test
    public void testIsVariantRow() {
        // Create a row type with variant metadata
        DataField field1 =
                new DataField(
                        0,
                        "age",
                        DataTypes.INT(),
                        VariantMetadataHelper.createVariantMetadata("$.age"));
        DataField field2 =
                new DataField(
                        1,
                        "name",
                        DataTypes.STRING(),
                        VariantMetadataHelper.createVariantMetadata("$.name"));
        RowType variantRowType = RowType.of(field1, field2);

        assertThat(VariantMetadataHelper.isVariantRowType(variantRowType)).isTrue();

        // Create a normal row type
        DataField normalField1 = new DataField(0, "age", DataTypes.INT());
        DataField normalField2 = new DataField(1, "name", DataTypes.STRING());
        RowType normalRowType = RowType.of(normalField1, normalField2);

        assertThat(VariantMetadataHelper.isVariantRowType(normalRowType)).isFalse();

        // Mixed row type (one field with metadata, one without)
        RowType mixedRowType = RowType.of(field1, normalField2);
        assertThat(VariantMetadataHelper.isVariantRowType(mixedRowType)).isFalse();

        // Empty row type
        RowType emptyRowType = RowType.of();
        assertThat(VariantMetadataHelper.isVariantRowType(emptyRowType)).isFalse();

        // Null row type
        assertThat(VariantMetadataHelper.isVariantRowType(null)).isFalse();
    }

    @Test
    public void testIsVariantField() {
        DataField variantField =
                new DataField(
                        0,
                        "age",
                        DataTypes.INT(),
                        VariantMetadataHelper.createVariantMetadata("$.age"));
        assertThat(VariantMetadataHelper.isVariantField(variantField)).isTrue();

        DataField normalField = new DataField(0, "age", DataTypes.INT());
        assertThat(VariantMetadataHelper.isVariantField(normalField)).isFalse();

        DataField fieldWithOtherDescription =
                new DataField(0, "age", DataTypes.INT(), "This is a regular comment");
        assertThat(VariantMetadataHelper.isVariantField(fieldWithOtherDescription)).isFalse();
    }

    @Test
    public void testExtractPath() {
        String description = VariantMetadataHelper.createVariantMetadata("$.a.b", true, "UTC");
        assertThat(VariantMetadataHelper.extractPath(description)).isEqualTo("$.a.b");

        assertThat(VariantMetadataHelper.extractPath("normal description")).isNull();
        assertThat(VariantMetadataHelper.extractPath(null)).isNull();
    }

    @Test
    public void testExtractFailOnError() {
        String description1 = VariantMetadataHelper.createVariantMetadata("$.field", true, "UTC");
        assertThat(VariantMetadataHelper.extractFailOnError(description1)).isTrue();

        String description2 = VariantMetadataHelper.createVariantMetadata("$.field", false, "UTC");
        assertThat(VariantMetadataHelper.extractFailOnError(description2)).isFalse();

        assertThat(VariantMetadataHelper.extractFailOnError("normal description")).isFalse();
        assertThat(VariantMetadataHelper.extractFailOnError(null)).isFalse();
    }

    @Test
    public void testExtractTimeZoneId() {
        String description1 =
                VariantMetadataHelper.createVariantMetadata("$.field", true, "Asia/Shanghai");
        assertThat(VariantMetadataHelper.extractTimeZoneId(description1))
                .isEqualTo("Asia/Shanghai");

        String description2 = VariantMetadataHelper.createVariantMetadata("$.field", false, "UTC");
        assertThat(VariantMetadataHelper.extractTimeZoneId(description2)).isEqualTo("UTC");

        assertThat(VariantMetadataHelper.extractTimeZoneId("normal description")).isEqualTo("UTC");
        assertThat(VariantMetadataHelper.extractTimeZoneId(null)).isEqualTo("UTC");
    }

    @Test
    public void testCreateVariantRowType() {
        RowType rowType =
                VariantMetadataHelper.VariantRowTypeBuilder.builder(false)
                        .field(DataTypes.INT(), "$.age", false, "UTC")
                        .field(DataTypes.STRING(), "$.name", false, "UTC")
                        .build();

        assertThat(VariantMetadataHelper.isVariantRowType(rowType)).isTrue();
        assertThat(rowType.getFieldCount()).isEqualTo(2);

        DataField field0 = rowType.getField(0);
        assertThat(VariantMetadataHelper.extractPath(field0.description())).isEqualTo("$.age");

        DataField field1 = rowType.getField(1);
        assertThat(VariantMetadataHelper.extractPath(field1.description())).isEqualTo("$.name");
    }

    @Test
    public void testExtractVariantFields() {
        // Create a variant row type using VariantRowTypeBuilder
        RowType variantRowType =
                VariantMetadataHelper.VariantRowTypeBuilder.builder(false)
                        .field(DataTypes.INT(), "$.age", true, "Asia/Shanghai")
                        .field(DataTypes.STRING(), "$.name", false, "UTC")
                        .build();

        // Extract back - now directly check fields from RowType
        assertThat(VariantMetadataHelper.isVariantRowType(variantRowType)).isTrue();
        assertThat(variantRowType.getFieldCount()).isEqualTo(2);

        DataField field0 = variantRowType.getField(0);
        assertThat(VariantMetadataHelper.extractPath(field0.description())).isEqualTo("$.age");
        assertThat(VariantMetadataHelper.extractFailOnError(field0.description())).isTrue();
        assertThat(VariantMetadataHelper.extractTimeZoneId(field0.description()))
                .isEqualTo("Asia/Shanghai");

        DataField field1 = variantRowType.getField(1);
        assertThat(VariantMetadataHelper.extractPath(field1.description())).isEqualTo("$.name");
        assertThat(VariantMetadataHelper.extractFailOnError(field1.description())).isFalse();
        assertThat(VariantMetadataHelper.extractTimeZoneId(field1.description())).isEqualTo("UTC");
    }

    @Test
    public void testExtractVariantFieldsFromNonVariantRow() {
        RowType normalRowType =
                RowType.of(
                        new DataField(0, "age", DataTypes.INT()),
                        new DataField(1, "name", DataTypes.STRING()));

        assertThat(VariantMetadataHelper.isVariantRowType(normalRowType)).isFalse();
    }

    @Test
    public void testPathWithSpecialCharacters() {
        String path = "$.a['b.c'].d[0]";
        String metadata = VariantMetadataHelper.createVariantMetadata(path, false, "UTC");
        assertThat(VariantMetadataHelper.extractPath(metadata)).isEqualTo(path);
    }

    @Test
    public void testDifferentTimeZones() {
        String[] timeZones = {"UTC", "Asia/Shanghai", "America/New_York", "Europe/London"};

        for (String tz : timeZones) {
            String metadata = VariantMetadataHelper.createVariantMetadata("$.field", true, tz);
            assertThat(VariantMetadataHelper.extractTimeZoneId(metadata)).isEqualTo(tz);
        }
    }
}
