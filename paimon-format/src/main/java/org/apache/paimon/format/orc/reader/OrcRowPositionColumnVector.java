package org.apache.paimon.format.orc.reader;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;

/** 1. */
public class OrcRowPositionColumnVector extends AbstractOrcColumnVector
        implements org.apache.paimon.data.columnar.LongColumnVector {
    private long startPosition;

    public OrcRowPositionColumnVector(ColumnVector ignore) {
        super(ignore);
    }

    @Override
    public long getLong(int i) {
        return startPosition + i;
    }

    public void setStartPosition(long startPosition) {
        this.startPosition = startPosition;
    }
}
