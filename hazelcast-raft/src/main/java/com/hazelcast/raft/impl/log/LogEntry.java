package com.hazelcast.raft.impl.log;

/**
 * Represents an entry in the {@link RaftLog}.
 * <p>
 * Each log entry stores a state machine command along with the term number
 * when the entry was received by the leader. The term numbers in log entries are used to detect inconsistencies
 * between logs. Each log entry also has an integer index identifying its position in the log.
 */
public class LogEntry {
    private int term;
    private long index;
    private Object operation;

    public LogEntry() {
    }

    public LogEntry(int term, long index, Object operation) {
        this.term = term;
        this.index = index;
        this.operation = operation;
    }

    public long index() {
        return index;
    }

    public int term() {
        return term;
    }

    public Object operation() {
        return operation;
    }

    @Override
    public String toString() {
        return "LogEntry{" + "term=" + term + ", index=" + index + ", operation=" + operation + '}';
    }
}
