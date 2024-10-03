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

package org.apache.paimon.types;

import javax.annotation.Nullable;

/** 1. */
public class VariantType extends DataType {

    private static final long serialVersionUID = 1L;

    private static final String FORMAT = "VARIANT";

    private @Nullable RowType writeShreddingSchema;

    private @Nullable RowType requiredSchema;

    private @Nullable RowType readShreddingSchema;

    public VariantType(boolean isNullable) {
        super(isNullable, DataTypeRoot.VARIANT);
    }

    public VariantType() {
        this(true);
    }

    public void setWriteShreddingSchema(RowType writeShreddingSchema) {
        this.writeShreddingSchema = writeShreddingSchema;
    }

    public RowType writeShreddingSchema() {
        return writeShreddingSchema;
    }

    public void setRequiredSchema(RowType requiredSchema) {
        this.requiredSchema = requiredSchema;
    }

    public RowType requiredSchema() {
        return requiredSchema;
    }

    public void setReadShreddingSchema(RowType readShreddingSchema) {
        this.readShreddingSchema = readShreddingSchema;
    }

    public RowType readShreddingSchema() {
        return readShreddingSchema;
    }

    @Override
    public int defaultSize() {
        return 0;
    }

    @Override
    public DataType copy(boolean isNullable) {
        VariantType variantType = new VariantType(isNullable);
        if (this.requiredSchema != null) {
            variantType.requiredSchema = this.requiredSchema;
        }
        return new VariantType(isNullable);
    }

    @Override
    public String asSQLString() {
        return withNullability(FORMAT);
    }

    @Override
    public <R> R accept(DataTypeVisitor<R> visitor) {
        return visitor.visit(this);
    }
}
