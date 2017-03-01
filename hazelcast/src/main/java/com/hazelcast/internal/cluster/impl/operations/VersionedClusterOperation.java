/*
 * Copyright (c) 2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.version.Version;

import java.io.IOException;

import static com.hazelcast.internal.cluster.impl.Versions.V3_9;


/**
 * TODO: Javadoc Pending...
 *
 * @since 3.9
 */
abstract class VersionedClusterOperation extends AbstractClusterOperation implements Versioned {

    private int memberListVersion;

    VersionedClusterOperation(int memberListVersion) {
        this.memberListVersion = memberListVersion;
    }

    int getMemberListVersion() {
        return memberListVersion;
    }

    @Override
    protected final void writeInternal(ObjectDataOutput out) throws IOException {
        writeInternalImpl(out);

        if (isGreaterOrEqual_V3_9(out.getVersion())) {
            out.writeInt(memberListVersion);
        }
    }

    abstract void writeInternalImpl(ObjectDataOutput out) throws IOException;

    @Override
    protected final void readInternal(ObjectDataInput in) throws IOException {
        readInternalImpl(in);

        if (isGreaterOrEqual_V3_9(in.getVersion())) {
            memberListVersion = in.readInt();
        }
    }

    abstract void readInternalImpl(ObjectDataInput in) throws IOException;

    final boolean isGreaterOrEqual_V3_9(Version version) {
        // in OSS, version is unknown
        return !BuildInfoProvider.BUILD_INFO.isEnterprise() || version.isGreaterOrEqual(V3_9);
    }
}
