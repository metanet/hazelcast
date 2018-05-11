package com.hazelcast.raft.service.session;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.service.session.operation.CloseSessionOp;
import com.hazelcast.raft.service.session.operation.CreateSessionOp;
import com.hazelcast.raft.service.session.operation.ExpireSessionsOp;
import com.hazelcast.raft.service.session.operation.KeepAliveAllSessionsOp;
import com.hazelcast.raft.service.session.operation.KeepAliveSessionOp;

public class RaftSessionDataSerializerHook implements DataSerializerHook {

    private static final int RAFT_SESSION_DS_FACTORY_ID = -2011;
    private static final String RAFT_SESSION_DS_FACTORY = "hazelcast.serialization.ds.raft.session";

    public static final int F_ID = FactoryIdHelper.getFactoryId(RAFT_SESSION_DS_FACTORY, RAFT_SESSION_DS_FACTORY_ID);

    public static final int RAFT_GROUP_SESSIONS_SNAPSHOT = 1;
    public static final int CREATE_SESSION = 2;
    public static final int KEEP_ALIVE_SESSION = 3;
    public static final int CLOSE_SESSION = 4;
    public static final int EXPIRE_SESSIONS = 5;
    public static final int KEEP_ALIVE_ALL_SESSIONS = 6;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        return new DataSerializableFactory() {
            @Override
            public IdentifiedDataSerializable create(int typeId) {
                switch (typeId) {
                    case RAFT_GROUP_SESSIONS_SNAPSHOT:
                        return new RaftSessionService.RaftGroupSessionsSnapshot();
                    case CREATE_SESSION:
                        return new CreateSessionOp();
                    case KEEP_ALIVE_SESSION:
                        return new KeepAliveSessionOp();
                    case CLOSE_SESSION:
                        return new CloseSessionOp();
                    case EXPIRE_SESSIONS:
                        return new ExpireSessionsOp();
                    case KEEP_ALIVE_ALL_SESSIONS:
                        return new KeepAliveAllSessionsOp();

                }
                throw new IllegalArgumentException("Undefined type: " + typeId);
            }
        };
    }
}
