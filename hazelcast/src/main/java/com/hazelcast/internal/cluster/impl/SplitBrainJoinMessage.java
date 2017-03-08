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

import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.version.MemberVersion;
import com.hazelcast.version.Version;

import java.io.IOException;
import java.util.Collection;

import static com.hazelcast.internal.cluster.Versions.V3_9;

/**
 * A {@code JoinMessage} issued by the master node of a subcluster to the master of another subcluster
 * while searching for other clusters for split brain recovery.
 */
public class SplitBrainJoinMessage extends JoinMessage implements Versioned {

    private Version clusterVersion;

    private int memberListVersion;

    public SplitBrainJoinMessage() {
    }

    public SplitBrainJoinMessage(byte packetVersion, int buildNumber, MemberVersion version, Address address, String uuid,
                                 boolean liteMember, ConfigCheck configCheck, Collection<Address> memberAddresses,
                                 int dataMemberCount, Version clusterVersion, int memberListVersion) {
        super(packetVersion, buildNumber, version, address, uuid, liteMember, configCheck, memberAddresses, dataMemberCount);
        this.clusterVersion = clusterVersion;
        this.memberListVersion = memberListVersion;
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        super.readData(in);
        clusterVersion = in.readObject();
        if (in.getVersion().isGreaterOrEqual(V3_9)) {
            memberListVersion = in.readInt();
        }
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        super.writeData(out);
        out.writeObject(clusterVersion);
        if (out.getVersion().isGreaterOrEqual(V3_9)) {
            out.writeInt(memberListVersion);
        }
    }

    @Override
    public String toString() {
        return "SplitBrainJoinMessage{"
                + "packetVersion=" + packetVersion
                + ", buildNumber=" + buildNumber
                + ", memberVersion=" + memberVersion
                + ", clusterVersion=" + clusterVersion
                + ", address=" + address
                + ", uuid='" + uuid + '\''
                + ", liteMember=" + liteMember
                + ", memberCount=" + getMemberCount()
                + ", dataMemberCount=" + dataMemberCount
                + ", memberListVersion=" + memberListVersion
                + '}';
    }

    @Override
    public int getFactoryId() {
        return ClusterDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return ClusterDataSerializerHook.SPLIT_BRAIN_JOIN_MESSAGE;
    }

    public Version getClusterVersion() {
        return clusterVersion;
    }

    public int getMemberListVersion() {
        return memberListVersion;
    }
}
