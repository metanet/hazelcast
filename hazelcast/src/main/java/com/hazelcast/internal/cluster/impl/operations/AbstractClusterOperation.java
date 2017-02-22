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

package com.hazelcast.internal.cluster.impl.operations;

import com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.spi.Operation;
import com.hazelcast.version.Version;

import java.io.IOException;

abstract class AbstractClusterOperation extends Operation implements JoinOperation, Versioned {

    /**
     * @since 3.9
     */
    private int version;

    AbstractClusterOperation(int version) {
        this.version = version;
    }

    int getVersion() {
        return version;
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public final String getServiceName() {
        return ClusterServiceImpl.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return ClusterDataSerializerHook.F_ID;
    }

    @Override
    protected final void writeInternal(ObjectDataOutput out) throws IOException {
        writeInternalImpl(out);

        if (out.getVersion().isGreaterOrEqual(Version.of(3, 9))) {
            out.writeInt(version);
        }
    }

    void writeInternalImpl(ObjectDataOutput out) throws IOException {
    }

    @Override
    protected final void readInternal(ObjectDataInput in) throws IOException {
        readInternalImpl(in);

        if (in.getVersion().isGreaterOrEqual(Version.of(3, 9))) {
            version = in.readInt();
        }
    }

    void readInternalImpl(ObjectDataInput in) throws IOException {
    }
}
