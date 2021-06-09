package org.example;

import org.apache.flink.walkthrough.common.entity.Transaction;

public class FraudProcessState {
    private long lastModified;
    private Transaction previous;

    public Transaction getPrevious( ) {
        return previous;
    }

    public void setPrevious( Transaction previous ) {
        this.previous = previous;
    }

    public long getLastModified( ) {
        return lastModified;
    }

    public void setLastModified( long lastModified ) {
        this.lastModified = lastModified;
    }
}
