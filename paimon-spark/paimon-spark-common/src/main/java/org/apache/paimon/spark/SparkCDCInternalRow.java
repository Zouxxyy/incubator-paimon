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

package org.apache.paimon.spark;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.spark.cdc.RowKindCol;
import org.apache.paimon.types.RowType;

import org.apache.spark.unsafe.types.UTF8String;

/** Spark {@link org.apache.spark.sql.catalyst.InternalRow} for paimon CDC InternalRow. */
public class SparkCDCInternalRow extends SparkInternalRow {

    public SparkCDCInternalRow(RowType rowType) {
        super(rowType);
    }

    private int getRowKindColOrdinal() {
        return row.getFieldCount() + RowKindCol.order();
    }

    @Override
    public boolean isNullAt(int ordinal) {
        if (ordinal == getRowKindColOrdinal()) {
            return false;
        } else {
            return row.isNullAt(ordinal);
        }
    }

    @Override
    public UTF8String getUTF8String(int ordinal) {
        if (ordinal == getRowKindColOrdinal()) {
            return fromPaimon(BinaryString.fromString(row.getRowKind().shortString()));
        } else {
            return fromPaimon(row.getString(ordinal));
        }
    }
}
