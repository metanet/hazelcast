/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.internal.cluster.impl;

import com.hazelcast.instance.MemberImpl;
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.internal.cluster.impl.operations.AuthenticationFailureOperation;
import com.hazelcast.internal.cluster.impl.operations.AuthorizationOperation;
import com.hazelcast.internal.cluster.impl.operations.BeforeJoinCheckFailureOperation;
import com.hazelcast.internal.cluster.impl.operations.ChangeClusterStateOperation;
import com.hazelcast.internal.cluster.impl.operations.ConfigMismatchOperation;
import com.hazelcast.internal.cluster.impl.operations.FetchMemberListStateOperation;
import com.hazelcast.internal.cluster.impl.operations.FinalizeJoinOperation;
import com.hazelcast.internal.cluster.impl.operations.GroupMismatchOperation;
import com.hazelcast.internal.cluster.impl.operations.HeartbeatOperation;
import com.hazelcast.internal.cluster.impl.operations.JoinRequestOperation;
import com.hazelcast.internal.cluster.impl.operations.LockClusterStateOperation;
import com.hazelcast.internal.cluster.impl.operations.MasterClaimOperation;
import com.hazelcast.internal.cluster.impl.operations.MasterConfirmationOperation;
import com.hazelcast.internal.cluster.impl.operations.MasterDiscoveryOperation;
import com.hazelcast.internal.cluster.impl.operations.MemberAttributeChangedOperation;
import com.hazelcast.internal.cluster.impl.operations.MembersUpdateOperation;
import com.hazelcast.internal.cluster.impl.operations.MemberRemoveOperation;
import com.hazelcast.internal.cluster.impl.operations.MergeClustersOperation;
import com.hazelcast.internal.cluster.impl.operations.PostJoinOperation;
import com.hazelcast.internal.cluster.impl.operations.RollbackClusterStateOperation;
import com.hazelcast.internal.cluster.impl.operations.SetMasterOperation;
import com.hazelcast.internal.cluster.impl.operations.ShutdownNodeOperation;
import com.hazelcast.internal.cluster.impl.operations.SplitBrainMergeValidationOperation;
import com.hazelcast.internal.cluster.impl.operations.TriggerMemberListPublishOperation;
import com.hazelcast.internal.partition.MigrationInfo;
import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.ArrayDataSerializableFactory;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.version.MemberVersion;
import com.hazelcast.version.Version;

public final class ClusterDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = 0;

    // never reassign numbers, in case of deletion leave a number gap.
    public static final int AUTH_FAILURE = 0;
    public static final int ADDRESS = 1;
    public static final int MEMBER = 2;
    public static final int HEARTBEAT = 3;
    public static final int CONFIG_CHECK = 4;
    public static final int BIND_MESSAGE = 5;
    public static final int MEMBER_INFO_UPDATE = 6;
    public static final int FINALIZE_JOIN = 7;
    public static final int AUTHORIZATION = 8;
    public static final int BEFORE_JOIN_CHECK_FAILURE = 9;
    public static final int CHANGE_CLUSTER_STATE = 10;
    public static final int CONFIG_MISMATCH = 11;
    public static final int GROUP_MISMATCH = 12;
    public static final int SPLIT_BRAIN_MERGE_VALIDATION = 13;
    public static final int JOIN_REQUEST_OP = 14;
    public static final int LOCK_CLUSTER_STATE = 15;
    public static final int MASTER_CLAIM = 16;
    public static final int MASTER_CONFIRM = 17;
    public static final int MASTER_DISCOVERY = 18;
    public static final int MEMBER_ATTR_CHANGED = 19;
    public static final int MEMBER_REMOVE = 20;
    public static final int MERGE_CLUSTERS = 21;
    public static final int POST_JOIN = 22;
    public static final int ROLLBACK_CLUSTER_STATE = 23;
    public static final int SET_MASTER = 24;
    public static final int SHUTDOWN_NODE = 25;
    public static final int TRIGGER_MEMBER_LIST_PUBLISH = 26;
    public static final int CLUSTER_STATE_TRANSACTION_LOG_RECORD = 27;
    public static final int MEMBER_INFO = 28;
    public static final int JOIN_MESSAGE = 29;
    public static final int JOIN_REQUEST = 30;
    public static final int MIGRATION_INFO = 31;
    public static final int MEMBER_VERSION = 32;
    public static final int CLUSTER_STATE_CHANGE = 33;
    public static final int SPLIT_BRAIN_JOIN_MESSAGE = 34;
    public static final int VERSION = 35;
    public static final int FETCH_MEMBER_LIST_STATE_OPERATION = 36;

    private static final int LEN = FETCH_MEMBER_LIST_STATE_OPERATION + 1;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        ConstructorFunction<Integer, IdentifiedDataSerializable>[] constructors = new ConstructorFunction[LEN];

        constructors[AUTH_FAILURE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new AuthenticationFailureOperation();
            }
        };
        constructors[ADDRESS] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new Address();
            }
        };
        constructors[MEMBER] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new MemberImpl();
            }
        };
        constructors[HEARTBEAT] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HeartbeatOperation();
            }
        };
        constructors[CONFIG_CHECK] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ConfigCheck();
            }
        };
        constructors[BIND_MESSAGE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new BindMessage();
            }
        };
        constructors[MEMBER_INFO_UPDATE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new MembersUpdateOperation();
            }
        };
        constructors[FINALIZE_JOIN] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new FinalizeJoinOperation();
            }
        };
        constructors[AUTHORIZATION] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new AuthorizationOperation();
            }
        };
        constructors[BEFORE_JOIN_CHECK_FAILURE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new BeforeJoinCheckFailureOperation();
            }
        };
        constructors[CHANGE_CLUSTER_STATE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ChangeClusterStateOperation();
            }
        };
        constructors[CONFIG_MISMATCH] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ConfigMismatchOperation();
            }
        };
        constructors[GROUP_MISMATCH] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new GroupMismatchOperation();
            }
        };
        constructors[SPLIT_BRAIN_MERGE_VALIDATION] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new SplitBrainMergeValidationOperation();
            }
        };
        constructors[JOIN_REQUEST_OP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new JoinRequestOperation();
            }
        };
        constructors[LOCK_CLUSTER_STATE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new LockClusterStateOperation();
            }
        };
        constructors[MASTER_CLAIM] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new MasterClaimOperation();
            }
        };
        constructors[MASTER_CONFIRM] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new MasterConfirmationOperation();
            }
        };
        constructors[MASTER_DISCOVERY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new MasterDiscoveryOperation();
            }
        };
        constructors[MEMBER_ATTR_CHANGED] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new MemberAttributeChangedOperation();
            }
        };
        constructors[MEMBER_REMOVE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new MemberRemoveOperation();
            }
        };
        constructors[MERGE_CLUSTERS] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new MergeClustersOperation();
            }
        };
        constructors[POST_JOIN] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new PostJoinOperation();
            }
        };
        constructors[ROLLBACK_CLUSTER_STATE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new RollbackClusterStateOperation();
            }
        };
        constructors[SET_MASTER] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new SetMasterOperation();
            }
        };
        constructors[SHUTDOWN_NODE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ShutdownNodeOperation();
            }
        };
        constructors[TRIGGER_MEMBER_LIST_PUBLISH] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new TriggerMemberListPublishOperation();
            }
        };
        constructors[CLUSTER_STATE_TRANSACTION_LOG_RECORD] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ClusterStateTransactionLogRecord();
            }
        };
        constructors[MEMBER_INFO] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new MemberInfo();
            }
        };
        constructors[JOIN_MESSAGE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new JoinMessage();
            }
        };
        constructors[JOIN_REQUEST] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new JoinRequest();
            }
        };
        constructors[MIGRATION_INFO] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new MigrationInfo();
            }
        };
        constructors[MEMBER_VERSION] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new MemberVersion();
            }
        };
        constructors[CLUSTER_STATE_CHANGE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ClusterStateChange();
            }
        };
        constructors[SPLIT_BRAIN_JOIN_MESSAGE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new SplitBrainJoinMessage();
            }
        };
        constructors[VERSION] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new Version();
            }
        };

        constructors[FETCH_MEMBER_LIST_STATE_OPERATION] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new FetchMemberListStateOperation();
            }
        };

        return new ArrayDataSerializableFactory(constructors);
    }
}
