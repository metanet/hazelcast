package com.hazelcast.raft.impl.serialization;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.nio.serialization.SerializerHook;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.raft.impl.command.ApplyRaftGroupMembersCommand;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedHashSet;

public class ApplyRaftGroupMembersCommandHook implements SerializerHook<ApplyRaftGroupMembersCommand> {
    @Override
    public Class<ApplyRaftGroupMembersCommand> getSerializationType() {
        return ApplyRaftGroupMembersCommand.class;
    }

    @Override
    public Serializer createSerializer() {
        return new StreamSerializer<ApplyRaftGroupMembersCommand>() {
            @Override
            public int getTypeId() {
                return SerializerHookConstants.APPLY_RAFT_GROUP_MEMBERS_OP;
            }

            @Override
            public void destroy() {
            }

            @Override
            public void write(ObjectDataOutput out, ApplyRaftGroupMembersCommand o) throws IOException {
                Collection<RaftEndpoint> members = o.getMembers();
                out.writeInt(members.size());
                for (RaftEndpoint endpoint : members) {
                    out.writeObject(endpoint);
                }
            }

            @Override
            public ApplyRaftGroupMembersCommand read(ObjectDataInput in) throws IOException {
                int count = in.readInt();
                Collection<RaftEndpoint> members = new LinkedHashSet<RaftEndpoint>();
                for (int i = 0; i < count; i++) {
                    RaftEndpoint endpoint = in.readObject();
                    members.add(endpoint);
                }
                return new ApplyRaftGroupMembersCommand(members);
            }
        };
    }

    @Override
    public boolean isOverwritable() {
        return false;
    }
}
