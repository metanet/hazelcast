package com.hazelcast.raft.command;

/**
 * A {@code RaftCommandOperation} to terminate an existing Raft group.
 */
public class TerminateRaftGroupCommand extends RaftCommand {

    public TerminateRaftGroupCommand() {
    }

    @Override
    public String toString() {
        return "TerminateRaftGroupCommand{}";
    }
}
