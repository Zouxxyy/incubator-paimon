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

package org.apache.paimon.data.columnar;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.columnar.BytesColumnVector.Bytes;
import org.apache.paimon.data.variant.PaimonShreddingUtils;
import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.data.variant.VariantSchema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.data.variant.PaimonShreddingUtils.METADATA_FIELD_NAME;
import static org.apache.paimon.data.variant.PaimonShreddingUtils.TYPED_VALUE_FIELD_NAME;
import static org.apache.paimon.data.variant.PaimonShreddingUtils.VARIANT_VALUE_FIELD_NAME;

/**
 * A VectorizedColumnBatch is a set of rows, organized with each column as a vector. It is the unit
 * of query execution, organized to minimize the cost per row.
 *
 * <p>{@code VectorizedColumnBatch}s are influenced by Apache Hive VectorizedRowBatch.
 */
public class VectorizedColumnBatch implements Serializable {

    private static final long serialVersionUID = 8180323238728166155L;

    /**
     * This number is carefully chosen to minimize overhead and typically allows one
     * VectorizedColumnBatch to fit in cache.
     */
    public static final int DEFAULT_SIZE = 2048;

    private int numRows;

    public final ColumnVector[] columns;

    public final VariantSchema[] variantsSchema;

    public VectorizedColumnBatch(ColumnVector[] vectors) {
        this.columns = vectors;
        this.variantsSchema = null;
    }

    public VectorizedColumnBatch(ColumnVector[] vectors, RowType[] variantsSchema) {
        this.columns = vectors;
        VariantSchema[] vs = new VariantSchema[vectors.length];
        for (int i = 0; i < vectors.length; i++) {
            RowType rowType = variantsSchema[i];
            if (rowType != null) {
                boolean noNeedValue = false;
                if (rowType.getFieldCount() == 1) {
                    noNeedValue = true;
                    List<DataField> fields = new ArrayList<>(3);
                    fields.add(new DataField(0, METADATA_FIELD_NAME, DataTypes.BYTES()));
                    fields.add(new DataField(1, VARIANT_VALUE_FIELD_NAME, DataTypes.BYTES()));
                    fields.add(
                            new DataField(2, TYPED_VALUE_FIELD_NAME, rowType.getField(2).type()));
                    rowType = new RowType(fields);
                }
                vs[i] = PaimonShreddingUtils.buildVariantSchema(rowType);
                if (noNeedValue) {
                    vs[i].setTypedIdx(0);
                }
            }
        }
        this.variantsSchema = vs;
    }

    public void setNumRows(int numRows) {
        this.numRows = numRows;
    }

    public int getNumRows() {
        return numRows;
    }

    public int getArity() {
        return columns.length;
    }

    public boolean isNullAt(int rowId, int colId) {
        return columns[colId].isNullAt(rowId);
    }

    public boolean getBoolean(int rowId, int colId) {
        return ((BooleanColumnVector) columns[colId]).getBoolean(rowId);
    }

    public byte getByte(int rowId, int colId) {
        return ((ByteColumnVector) columns[colId]).getByte(rowId);
    }

    public short getShort(int rowId, int colId) {
        return ((ShortColumnVector) columns[colId]).getShort(rowId);
    }

    public int getInt(int rowId, int colId) {
        return ((IntColumnVector) columns[colId]).getInt(rowId);
    }

    public long getLong(int rowId, int colId) {
        return ((LongColumnVector) columns[colId]).getLong(rowId);
    }

    public float getFloat(int rowId, int colId) {
        return ((FloatColumnVector) columns[colId]).getFloat(rowId);
    }

    public double getDouble(int rowId, int colId) {
        return ((DoubleColumnVector) columns[colId]).getDouble(rowId);
    }

    public BinaryString getString(int rowId, int pos) {
        Bytes byteArray = getByteArray(rowId, pos);
        return BinaryString.fromBytes(byteArray.data, byteArray.offset, byteArray.len);
    }

    public byte[] getBinary(int rowId, int pos) {
        Bytes byteArray = getByteArray(rowId, pos);
        if (byteArray.len == byteArray.data.length) {
            return byteArray.data;
        } else {
            byte[] ret = new byte[byteArray.len];
            System.arraycopy(byteArray.data, byteArray.offset, ret, 0, byteArray.len);
            return ret;
        }
    }

    public Bytes getByteArray(int rowId, int colId) {
        return ((BytesColumnVector) columns[colId]).getBytes(rowId);
    }

    public Decimal getDecimal(int rowId, int colId, int precision, int scale) {
        return ((DecimalColumnVector) (columns[colId])).getDecimal(rowId, precision, scale);
    }

    public Timestamp getTimestamp(int rowId, int colId, int precision) {
        return ((TimestampColumnVector) (columns[colId])).getTimestamp(rowId, precision);
    }

    public InternalArray getArray(int rowId, int colId) {
        return ((ArrayColumnVector) columns[colId]).getArray(rowId);
    }

    public InternalRow getRow(int rowId, int colId) {
        return ((RowColumnVector) columns[colId]).getRow(rowId);
    }

    public Variant getVariant(int rowId, int colId) {
        if (variantsSchema[colId] != null) {
            return PaimonShreddingUtils.rebuild(getRow(rowId, colId), variantsSchema[colId]);
        }
        InternalRow row = getRow(rowId, colId);
        byte[] value = row.getBinary(0);
        byte[] metadata = row.getBinary(1);
        return new Variant(value, metadata);
    }

    public InternalMap getMap(int rowId, int colId) {
        return ((MapColumnVector) columns[colId]).getMap(rowId);
    }

    public VectorizedColumnBatch copy(ColumnVector[] vectors) {
        VectorizedColumnBatch vectorizedColumnBatch = new VectorizedColumnBatch(vectors);
        vectorizedColumnBatch.setNumRows(numRows);
        return vectorizedColumnBatch;
    }
}
