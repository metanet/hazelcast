package com.hazelcast.raft.impl.command;

import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.raft.command.RaftCommand;

import java.util.Collection;

/**
 * A {@code RaftCommand} to update members of an existing Raft group. This operation is generated
 * as a result of member add or remove request.
 */
public class ApplyRaftGroupMembersCommand extends RaftCommand {

    private Collection<RaftEndpoint> members;

    public ApplyRaftGroupMembersCommand(Collection<RaftEndpoint> members) {
        this.members = members;
    }

    public Collection<RaftEndpoint> getMembers() {
        return members;
    }

    @Override
    public String toString() {
        return "ApplyRaftGroupMembersCommand{" + "members=" + members + '}';
    }
}
