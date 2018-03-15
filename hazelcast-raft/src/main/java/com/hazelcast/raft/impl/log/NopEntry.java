package com.hazelcast.raft.impl.log;

/**
 * No-op entry operation which is appended in the Raft log when a new leader is elected and
 * {@link com.hazelcast.raft.RaftConfig#appendNopEntryOnLeaderElection} is enabled.
 */
public class NopEntry {
}
