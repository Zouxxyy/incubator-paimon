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

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.apache.paimon.types.DataTypesTest.assertThat;

/** Test of {@link VariantFunctions}. */
public class VariantFunctionsTest {

    @Test
    public void testVariantGet() {
        String json =
                "{\n"
                        + "  \"object\": {\n"
                        + "    \"name\": \"John Doe\",\n"
                        + "    \"age\": 30,\n"
                        + "    \"isEmployed\": true,\n"
                        + "    \"address\": {\n"
                        + "      \"street\": \"123 Main St\",\n"
                        + "      \"city\": \"New York\"\n"
                        + "    }\n"
                        + "  },\n"
                        + "  \"array\": [1, 2, 3, 4, 5],\n"
                        + "  \"string\": \"Hello, World!\",\n"
                        + "  \"long\": 12345678901234,\n"
                        + "  \"double\": 1.0123456789012345678901234567890123456789,\n"
                        + "  \"decimal\": 100.99,\n"
                        + "  \"boolean1\": true,\n"
                        + "  \"boolean2\": false,\n"
                        + "  \"nullField\": null\n"
                        + "}\n";

        Variant variant = Variant.fromJson(json);
        assertThat(variant.variantGet("$.object"))
                .isEqualTo(
                        "{\"address\":{\"city\":\"New York\",\"street\":\"123 Main St\"},\"age\":30,\"isEmployed\":true,\"name\":\"John Doe\"}");
        assertThat(variant.variantGet("$.object.name")).isEqualTo("John Doe");
        assertThat(variant.variantGet("$.object.address.street")).isEqualTo("123 Main St");
        assertThat(variant.variantGet("$[\"object\"]['address'].city")).isEqualTo("New York");
        assertThat(variant.variantGet("$.array")).isEqualTo("[1,2,3,4,5]");
        assertThat(variant.variantGet("$.array[0]")).isEqualTo(1L);
        assertThat(variant.variantGet("$.array[3]")).isEqualTo(4L);
        assertThat(variant.variantGet("$.string")).isEqualTo("Hello, World!");
        assertThat(variant.variantGet("$.long")).isEqualTo(12345678901234L);
        assertThat(variant.variantGet("$.double"))
                .isEqualTo(1.0123456789012345678901234567890123456789);
        assertThat(variant.variantGet("$.decimal")).isEqualTo(new BigDecimal("100.99"));
        assertThat(variant.variantGet("$.boolean1")).isEqualTo(true);
        assertThat(variant.variantGet("$.boolean2")).isEqualTo(false);
        assertThat(variant.variantGet("$.nullField")).isNull();
    }

    @Test
    public void testShredding() {
        Variant variant = Variant.fromJson("{\"a\": 1, \"b\": \"hello\"}");

        // Happy path
        RowType r1 =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.STRING()},
                        new String[] {"a", "b"});
        GenericRow expert1 =
                GenericRow.of(
                        variant.getMetadata(),
                        null,
                        GenericRow.of(
                                GenericRow.of(null, 1),
                                GenericRow.of(null, BinaryString.fromString("hello"))));
        testShreddingWithSchema(variant, r1, expert1);
        testRebuildShredding(expert1, r1, variant);

        // Missing field
        RowType r2 =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.STRING(), DataTypes.STRING()},
                        new String[] {"a", "c", "b"});
        GenericRow expert2 =
                GenericRow.of(
                        variant.getMetadata(),
                        null,
                        GenericRow.of(
                                GenericRow.of(null, 1),
                                GenericRow.of(null, null),
                                GenericRow.of(null, BinaryString.fromString("hello"))));
        testShreddingWithSchema(variant, r2, expert2);

        // "a" is not present in shredding schema
        RowType r3 =
                RowType.of(
                        new DataType[] {DataTypes.STRING(), DataTypes.STRING()},
                        new String[] {"b", "c"});
        GenericRow expert3 =
                GenericRow.of(
                        variant.getMetadata(),
                        untypedValue("{\"a\": 1}"),
                        GenericRow.of(
                                GenericRow.of(null, BinaryString.fromString("hello")),
                                GenericRow.of(null, null)));
        testShreddingWithSchema(variant, r3, expert3);
    }

    private byte[] untypedValue(String input) {
        return Variant.fromJson(input).getValue();
    }

    private void testShreddingWithSchema(Variant variant, RowType rowType, InternalRow expected) {
        RowType shreddingSchema = PaimonShreddingUtils.variantShreddingSchema(rowType);
        VariantSchema variantSchema = PaimonShreddingUtils.buildVariantSchema(shreddingSchema);
        InternalRow actual = PaimonShreddingUtils.castShredded(variant, variantSchema);
        assertThat(actual).isEqualTo(expected);
    }

    private void testRebuildShredding(InternalRow shreddedRow, RowType rowType, Variant expected) {
        RowType shreddingSchema = PaimonShreddingUtils.variantShreddingSchema(rowType);
        VariantSchema variantSchema = PaimonShreddingUtils.buildVariantSchema(shreddingSchema);
        Variant variant = PaimonShreddingUtils.rebuild(shreddedRow, variantSchema);
        assertThat(variant).isEqualTo(expected);
    }
}
