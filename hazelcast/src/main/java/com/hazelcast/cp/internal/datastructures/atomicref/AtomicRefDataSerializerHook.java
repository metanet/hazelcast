/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.datastructures.atomicref;

import com.hazelcast.cp.internal.datastructures.atomicref.fencedref.AttemptFenceFn;
import com.hazelcast.cp.internal.datastructures.atomicref.fencedref.CASFencedValFn;
import com.hazelcast.cp.internal.datastructures.atomicref.fencedref.FencedVal;
import com.hazelcast.cp.internal.datastructures.atomicref.fencedref.GetFencedValFn;
import com.hazelcast.cp.internal.datastructures.atomicref.fencedref.SetFencedValFn;
import com.hazelcast.cp.internal.datastructures.atomicref.operation.ApplyOp;
import com.hazelcast.cp.internal.datastructures.atomicref.operation.CompareAndSetOp;
import com.hazelcast.cp.internal.datastructures.atomicref.operation.ContainsOp;
import com.hazelcast.cp.internal.datastructures.atomicref.operation.GetOp;
import com.hazelcast.cp.internal.datastructures.atomicref.operation.SetOp;
import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;

@SuppressWarnings("checkstyle:declarationorder")
public final class AtomicRefDataSerializerHook implements DataSerializerHook {

    private static final int RAFT_ATOMIC_REF_DS_FACTORY_ID = -1014;
    private static final String RAFT_ATOMIC_REF_DS_FACTORY = "hazelcast.serialization.ds.raft.atomicref";

    public static final int F_ID = FactoryIdHelper.getFactoryId(RAFT_ATOMIC_REF_DS_FACTORY, RAFT_ATOMIC_REF_DS_FACTORY_ID);

    public static final int SNAPSHOT = 1;
    public static final int APPLY_OP = 2;
    public static final int COMPARE_AND_SET_OP = 3;
    public static final int CONTAINS_OP = 4;
    public static final int GET_OP = 5;
    public static final int SET_OP = 6;
    public static final int FENCED_VAL = 7;
    public static final int ATTEMPT_FENCE_FN = 8;
    public static final int CAS_FENCED_VAL_FN = 9;
    public static final int GET_FENCED_VAL_FN = 10;
    public static final int SET_FENCED_VAL_FN = 11;


    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        return typeId -> {
            switch (typeId) {
                case SNAPSHOT:
                    return new AtomicRefSnapshot();
                case APPLY_OP:
                    return new ApplyOp();
                case COMPARE_AND_SET_OP:
                    return new CompareAndSetOp();
                case CONTAINS_OP:
                    return new ContainsOp();
                case GET_OP:
                    return new GetOp();
                case SET_OP:
                    return new SetOp();
                case FENCED_VAL:
                    return new FencedVal();
                case ATTEMPT_FENCE_FN:
                    return new AttemptFenceFn();
                case CAS_FENCED_VAL_FN:
                    return new CASFencedValFn();
                case GET_FENCED_VAL_FN:
                    return new GetFencedValFn();
                case SET_FENCED_VAL_FN:
                    return new SetFencedValFn();
                default:
                    throw new IllegalArgumentException("Undefined type: " + typeId);
            }
        };
    }
}
