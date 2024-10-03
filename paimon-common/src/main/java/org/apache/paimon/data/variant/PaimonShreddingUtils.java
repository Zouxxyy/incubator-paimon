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
import org.apache.paimon.data.DataGetters;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarBinaryType;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

/** 1. */
public class PaimonShreddingUtils {

    /** 1. */
    static class PaimonShreddedRow implements ShreddingUtils.ShreddedRow {

        private final DataGetters row;

        public PaimonShreddedRow(DataGetters row) {
            this.row = row;
        }

        @Override
        public boolean isNullAt(int ordinal) {
            return row.isNullAt(ordinal);
        }

        @Override
        public boolean getBoolean(int ordinal) {
            return row.getBoolean(ordinal);
        }

        @Override
        public byte getByte(int ordinal) {
            return row.getByte(ordinal);
        }

        @Override
        public short getShort(int ordinal) {
            return row.getShort(ordinal);
        }

        @Override
        public int getInt(int ordinal) {
            return row.getInt(ordinal);
        }

        @Override
        public long getLong(int ordinal) {
            return row.getLong(ordinal);
        }

        @Override
        public float getFloat(int ordinal) {
            return row.getFloat(ordinal);
        }

        @Override
        public double getDouble(int ordinal) {
            return row.getDouble(ordinal);
        }

        @Override
        public BigDecimal getDecimal(int ordinal, int precision, int scale) {
            throw new RuntimeException();
            // return row.getDecimal(ordinal, precision, scale);
        }

        @Override
        public String getString(int ordinal) {
            return row.getString(ordinal).toString();
        }

        @Override
        public byte[] getBinary(int ordinal) {
            return row.getBinary(ordinal);
        }

        @Override
        public ShreddingUtils.ShreddedRow getStruct(int ordinal, int numFields) {
            return new PaimonShreddedRow(row.getRow(ordinal, numFields));
        }

        @Override
        public ShreddingUtils.ShreddedRow getArray(int ordinal) {
            return new PaimonShreddedRow(row.getArray(ordinal));
        }

        @Override
        public int numElements() {
            return ((InternalArray) row).size();
        }
    }

    public static final String METADATA_FIELD_NAME = "metadata";
    public static final String VARIANT_VALUE_FIELD_NAME = "value";
    public static final String TYPED_VALUE_FIELD_NAME = "typed_value";

    public static RowType variantShreddingSchema(RowType rowType) {
        return variantShreddingSchema(rowType, true);
    }

    public static RowType variantShreddingSchema(DataType dataType, boolean topLevel) {
        RowType.Builder builder = RowType.builder();
        if (topLevel) {
            builder.field(METADATA_FIELD_NAME, DataTypes.BYTES());
        }
        switch (dataType.getTypeRoot()) {
            case ARRAY:
                ArrayType arrayType = (ArrayType) dataType;
                ArrayType shreddedArrayType =
                        new ArrayType(
                                arrayType.isNullable(),
                                variantShreddingSchema(arrayType.getElementType(), false));
                builder.field(VARIANT_VALUE_FIELD_NAME, DataTypes.BYTES());
                builder.field(TYPED_VALUE_FIELD_NAME, shreddedArrayType);
                break;
            case ROW:
                RowType rowType = (RowType) dataType;
                RowType shreddedRowType =
                        rowType.copy(
                                rowType.getFields().stream()
                                        .map(
                                                field ->
                                                        field.newType(
                                                                variantShreddingSchema(
                                                                        field.type(), false)))
                                        .collect(Collectors.toList()));
                builder.field(VARIANT_VALUE_FIELD_NAME, DataTypes.BYTES());
                builder.field(TYPED_VALUE_FIELD_NAME, shreddedRowType);
                break;
            case VARIANT:
                builder.field(VARIANT_VALUE_FIELD_NAME, DataTypes.BYTES());
                break;
            case CHAR:
            case VARCHAR:
            case BOOLEAN:
            case BINARY:
            case VARBINARY:
            case DECIMAL:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
                builder.field(VARIANT_VALUE_FIELD_NAME, DataTypes.BYTES());
                builder.field(TYPED_VALUE_FIELD_NAME, dataType);
                break;
            default:
                throw new IllegalArgumentException(
                        "Invalid variant shredding schema for data type: " + dataType);
        }
        return builder.build();
    }

    public static VariantSchema buildVariantSchema(RowType rowType) {
        return buildVariantSchema(rowType, true);
    }

    public static VariantSchema buildVariantSchema(RowType rowType, boolean topLevel) {
        int typedIdx = -1;
        int variantIdx = -1;
        int topLevelMetadataIdx = -1;
        VariantSchema.ScalarType scalarSchema = null;
        VariantSchema.ObjectField[] objectSchema = null;
        VariantSchema arraySchema = null;

        List<DataField> fields = rowType.getFields();
        for (int i = 0; i < fields.size(); i++) {
            DataField field = fields.get(i);
            DataType dataType = field.type();
            switch (field.name()) {
                case TYPED_VALUE_FIELD_NAME:
                    if (typedIdx != -1) {
                        throw new RuntimeException();
                    }
                    typedIdx = i;
                    switch (field.type().getTypeRoot()) {
                        case ROW:
                            RowType r = (RowType) dataType;
                            List<DataField> rFields = r.getFields();
                            objectSchema = new VariantSchema.ObjectField[rFields.size()];
                            for (int index = 0; index < rFields.size(); index++) {
                                if (field.type() instanceof RowType) {
                                    DataField f = rFields.get(index);
                                    objectSchema[index] =
                                            new VariantSchema.ObjectField(
                                                    f.name(),
                                                    buildVariantSchema((RowType) f.type(), false));
                                } else {
                                    throw new RuntimeException();
                                }
                            }
                            break;
                        case ARRAY:
                            ArrayType arrayType = (ArrayType) dataType;
                            if (arrayType.getElementType() instanceof RowType) {
                                arraySchema =
                                        buildVariantSchema(
                                                (RowType) arrayType.getElementType(), false);
                            } else {
                                throw new RuntimeException();
                            }
                            break;
                        case BOOLEAN:
                            scalarSchema = new VariantSchema.BooleanType();
                            break;
                        case TINYINT:
                            scalarSchema =
                                    new VariantSchema.IntegralType(VariantSchema.IntegralSize.BYTE);
                            break;
                        case SMALLINT:
                            scalarSchema =
                                    new VariantSchema.IntegralType(
                                            VariantSchema.IntegralSize.SHORT);
                            break;
                        case INTEGER:
                            scalarSchema =
                                    new VariantSchema.IntegralType(VariantSchema.IntegralSize.INT);
                            break;
                        case BIGINT:
                            scalarSchema =
                                    new VariantSchema.IntegralType(VariantSchema.IntegralSize.LONG);
                            break;
                        case FLOAT:
                            scalarSchema = new VariantSchema.FloatType();
                            break;
                        case DOUBLE:
                            scalarSchema = new VariantSchema.DoubleType();
                            break;
                        case VARCHAR:
                            scalarSchema = new VariantSchema.StringType();
                            break;
                        case BINARY:
                            scalarSchema = new VariantSchema.BinaryType();
                            break;
                        case DATE:
                            scalarSchema = new VariantSchema.DateType();
                            break;
                        case DECIMAL:
                            DecimalType d = (DecimalType) dataType;
                            scalarSchema =
                                    new VariantSchema.DecimalType(d.getPrecision(), d.getScale());
                            break;
                        default:
                            throw new RuntimeException();
                    }
                    break;

                case VARIANT_VALUE_FIELD_NAME:
                    if (variantIdx != -1 || !(field.type() instanceof VarBinaryType)) {
                        throw new RuntimeException();
                    }
                    variantIdx = i;
                    break;

                case METADATA_FIELD_NAME:
                    if (topLevelMetadataIdx != -1 || !(field.type() instanceof VarBinaryType)) {
                        throw new RuntimeException();
                    }
                    topLevelMetadataIdx = i;
                    break;

                default:
                    throw new RuntimeException();
            }

            if (topLevel && (topLevelMetadataIdx == -1)) {
                topLevelMetadataIdx = i;
            }
        }

        if (topLevel != (topLevelMetadataIdx >= 0)) {
            throw new RuntimeException();
        }

        return new VariantSchema(
                typedIdx,
                variantIdx,
                topLevelMetadataIdx,
                fields.size(),
                scalarSchema,
                objectSchema,
                arraySchema);
    }

    /** 1. */
    public static class PaimonShreddedResult implements VariantShreddingWriter.ShreddedResult {

        private final VariantSchema schema;
        // Result is stored as an InternalRow.
        private final GenericRow row;

        public PaimonShreddedResult(VariantSchema schema) {
            this.schema = schema;
            this.row = new GenericRow(schema.numFields);
        }

        @Override
        public void addArray(VariantShreddingWriter.ShreddedResult[] array) {
            GenericArray arrayResult =
                    new GenericArray(
                            java.util.Arrays.stream(array)
                                    .map(result -> ((PaimonShreddedResult) result).row)
                                    .toArray(InternalRow[]::new));
            row.setField(schema.typedIdx, arrayResult);
        }

        @Override
        public void addObject(VariantShreddingWriter.ShreddedResult[] values) {
            GenericRow innerRow = new GenericRow(schema.objectSchema.length);
            for (int i = 0; i < values.length; i++) {
                innerRow.setField(i, ((PaimonShreddedResult) values[i]).row);
            }
            row.setField(schema.typedIdx, innerRow);
        }

        @Override
        public void addVariantValue(byte[] result) {
            row.setField(schema.variantIdx, result);
        }

        @Override
        public void addScalar(Object result) {
            Object paimonValue;
            if (schema.scalarSchema instanceof VariantSchema.StringType) {
                paimonValue = BinaryString.fromString((String) result);
            } else {
                paimonValue = result;
            }
            row.setField(schema.typedIdx, paimonValue);
        }

        @Override
        public void addMetadata(byte[] result) {
            row.setField(schema.topLevelMetadataIdx, result);
        }
    }

    /** 1. */
    public static class PaimonShreddedResultBuilder
            implements VariantShreddingWriter.ShreddedResultBuilder {
        @Override
        public VariantShreddingWriter.ShreddedResult createEmpty(VariantSchema schema) {
            return new PaimonShreddedResult(schema);
        }

        // Consider allowing this to be set via config?
        @Override
        public boolean allowNumericScaleChanges() {
            return true;
        }
    }

    public static InternalRow castShredded(Variant variant, VariantSchema variantSchema) {
        return ((PaimonShreddedResult)
                        VariantShreddingWriter.castShredded(
                                variant, variantSchema, new PaimonShreddedResultBuilder()))
                .row;
    }

    public static Variant rebuild(InternalRow row, VariantSchema variantSchema) {
        return ShreddingUtils.rebuild(new PaimonShreddedRow(row), variantSchema);
    }

    public static RowType fromString(String s) {
        RowType.Builder builder = RowType.builder();
        for (String name : s.split(",")) {
            String[] split = name.split(":");
            builder.field(split[0], DataTypes.STRING());
        }
        return builder.build();
    }

    public static RowType pruneShreddingSchema(RowType shreddingSchema, RowType requiredSchema) {
        if (requiredSchema == null) {
            return shreddingSchema;
        }
        List<DataField> newFields = new ArrayList<>();
        HashSet<String> requiredFieldNames = new HashSet<>(requiredSchema.getFieldNames());
        boolean notContains = false;
        DataField typedValue = shreddingSchema.getField(TYPED_VALUE_FIELD_NAME);
        RowType typedValueFields = (RowType) typedValue.type();
        List<DataField> newTypedValueFields = new ArrayList<>();
        for (String requiredFieldName : requiredFieldNames) {
            if (typedValueFields.containsField(requiredFieldName)) {
                newTypedValueFields.add(typedValueFields.getField(requiredFieldName));
            } else {
                notContains = true;
            }
        }
        if (notContains) {
            newFields.add(shreddingSchema.getField(METADATA_FIELD_NAME));
            newFields.add(shreddingSchema.getField(VARIANT_VALUE_FIELD_NAME));
        }
        if (!newTypedValueFields.isEmpty()) {
            newFields.add(typedValue.newType(new RowType(newTypedValueFields)));
        }
        return new RowType(newFields);
    }
}
