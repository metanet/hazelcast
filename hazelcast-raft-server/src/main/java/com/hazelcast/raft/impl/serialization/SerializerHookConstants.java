package com.hazelcast.raft.impl.serialization;

public final class SerializerHookConstants {

    public static final int RAFT_RESERVED_SPACE_START = -400;


    public static final int RAFT_ENDPOINT = -400;
    public static final int PRE_VOTE_REQUEST = -401;
    public static final int PRE_VOTE_RESPONSE = -402;
    public static final int VOTE_REQUEST = -403;
    public static final int VOTE_RESPONSE = -404;
    public static final int APPEND_REQUEST = -405;
    public static final int APPEND_SUCCESS_RESPONSE = -406;
    public static final int APPEND_FAILURE_RESPONSE = -407;
    public static final int LOG_ENTRY = -408;
    public static final int SNAPSHOT_ENTRY = -409;
    public static final int INSTALL_SNAPSHOT = -410;
    public static final int TERMINATE_RAFT_GROUP_OP = -411;
    public static final int APPLY_RAFT_GROUP_MEMBERS_OP = -412;
    public static final int NOP_ENTRY_OP = -413;

    private SerializerHookConstants() {
    }
}
