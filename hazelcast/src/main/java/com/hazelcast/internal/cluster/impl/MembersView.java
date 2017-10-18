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
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static com.hazelcast.internal.cluster.Versions.V3_10;
import static com.hazelcast.util.Preconditions.checkFalse;
import static com.hazelcast.util.Preconditions.checkTrue;
import static java.util.Collections.sort;
import static java.util.Collections.unmodifiableMap;

/**
 * MembersView is a container object to carry member list and version together.
 */
public final class MembersView implements IdentifiedDataSerializable {

    private int version;
    private Map<MemberInfo, Integer> memberJoinVersions;

    public MembersView() {
    }

    // MembersUpdateOp ve decideNewMembersView()'ta cagriliyor, versions olacak burada da
    public MembersView(int version, Map<MemberInfo, Integer> memberJoinVersions) {
        checkTrue(memberJoinVersions.size() == new HashSet<Integer>(memberJoinVersions.values()).size(),
                "JoinVersionMap: " + memberJoinVersions + " has duplicate join versions!");

        for (Entry<MemberInfo, Integer> e : memberJoinVersions.entrySet()) {
            int memberJoinVersion = e.getValue();
            checkFalse(version < memberJoinVersion, "Version: " + version
                    + " cannot be smaller than join version: " + e.getValue() + " of " + e.getKey());
        }

        this.version = version;
        this.memberJoinVersions = unmodifiableMap(sortMembersByJoinOrder(memberJoinVersions));
    }

    private Map<MemberInfo, Integer> sortMembersByJoinOrder(final Map<MemberInfo, Integer> memberJoinVersions) {
        List<MemberInfo> sorted = new ArrayList<MemberInfo>(memberJoinVersions.keySet());
        sort(sorted, new Comparator<MemberInfo>() {
            @Override
            public int compare(MemberInfo m1, MemberInfo m2) {
                return memberJoinVersions.get(m1).compareTo(memberJoinVersions.get(m2));
            }
        });

        LinkedHashMap<MemberInfo, Integer> joinVersions = new LinkedHashMap<MemberInfo, Integer>(memberJoinVersions.size());
        for (MemberInfo member : sorted) {
            joinVersions.put(member, memberJoinVersions.get(member));
        }

        return joinVersions;
    }

    // join'de yeni member'lar eklenirken cagriliyor. burada version'lari ayarlayacagiz.
    /**
     * Creates clone of source {@link MembersView} additionally including new members.
     *
     * @param source     source map
     * @param newMembers new members to add
     * @return clone map
     */
    static MembersView cloneAdding(MembersView source, Collection<MemberInfo> newMembers) {
        Map<MemberInfo, Integer> joinVersions = new HashMap<MemberInfo, Integer>(source.memberJoinVersions);
        int version = source.version;
        for (MemberInfo member : newMembers) {
            joinVersions.put(member, ++version);
        }

        return new MembersView(version, joinVersions);
    }

    // handleMastershipClaim()'de cagriliyor, burada version'lar da gelecek
    /**
     * Creates a new {@code MemberMap} including given members.
     *
     * @param version version
     * @param memberJoinVersions memberJoinVersions
     * @return a new {@code MemberMap}
     */
    public static MembersView createNew(int version, Map<MemberImpl, Integer> memberJoinVersions) {
        Map<MemberInfo, Integer> versions = new HashMap<MemberInfo, Integer>();
        for (Entry<MemberImpl, Integer> e : memberJoinVersions.entrySet()) {
            versions.put(new MemberInfo(e.getKey()), e.getValue());
        }

        return new MembersView(version, versions);
    }

    public List<MemberInfo> getMembers() {
        return new ArrayList<MemberInfo>(memberJoinVersions.keySet());
    }

    public int getMemberJoinVersion(MemberInfo memberInfo) {
        Integer version = memberJoinVersions.get(memberInfo);
        if (version == null) {
            throw new IllegalArgumentException(memberInfo + " is not present in " + this);
        }

        return version;
    }

    public Map<MemberInfo, Integer> getMemberJoinVersions() {
        return memberJoinVersions;
    }

    public int size() {
        return memberJoinVersions.size();
    }

    public int getVersion() {
        return version;
    }

    // updateMembers()'ta cagriliyor
    MemberMap toMemberMap() {
        Map<MemberImpl, Integer> joinVersionMap = new LinkedHashMap<MemberImpl, Integer>(memberJoinVersions.size());
        for (Entry<MemberInfo, Integer> e : memberJoinVersions.entrySet()) {
            joinVersionMap.put(e.getKey().toMember(), e.getValue());
        }
        return MemberMap.createNew(version, joinVersionMap);
    }

    public boolean containsAddress(Address address) {
        for (MemberInfo member : memberJoinVersions.keySet()) {
            if (member.getAddress().equals(address)) {
                return true;
            }
        }

        return false;
    }

    public boolean containsMember(Address address, String uuid) {
        for (MemberInfo member : memberJoinVersions.keySet()) {
            if (member.getAddress().equals(address)) {
                return member.getUuid().equals(uuid);
            }
        }

        return false;
    }

    public Set<Address> getAddresses() {
        Set<Address> addresses = new HashSet<Address>(memberJoinVersions.size());
        for (MemberInfo member : memberJoinVersions.keySet()) {
            addresses.add(member.getAddress());
        }
        return addresses;
    }

    public MemberInfo getMember(Address address) {
        for (MemberInfo member : memberJoinVersions.keySet()) {
            if (member.getAddress().equals(address)) {
                return member;
            }
        }

        return null;
    }

    public boolean isLaterThan(MembersView other) {
        return version > other.version;
    }

    @Override
    public int getFactoryId() {
        return ClusterDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return ClusterDataSerializerHook.MEMBERS_VIEW;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        out.writeInt(version);
        out.writeInt(memberJoinVersions.size());
        if (out.getVersion().isGreaterOrEqual(V3_10)) {
            for (Entry<MemberInfo, Integer> e : memberJoinVersions.entrySet()) {
                e.getKey().writeData(out);
                out.writeInt(e.getValue());
            }
        } else {
            // TODO basri this is broken.
            for (MemberInfo member : memberJoinVersions.keySet()) {
                member.writeData(out);
            }
        }
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        version = in.readInt();
        int size = in.readInt();
        Map<MemberInfo, Integer> memberJoinVersions = new LinkedHashMap<MemberInfo, Integer>();
        boolean readJoinVersions = in.getVersion().isGreaterOrEqual(V3_10);
        for (int i = 0; i < size; i++) {
            MemberInfo member = new MemberInfo();
            member.readData(in);
            // TODO basri this is broken.
            int joinVersion = readJoinVersions ? in.readInt() : 0;
            memberJoinVersions.put(member, joinVersion);
        }

        this.memberJoinVersions = unmodifiableMap(memberJoinVersions);
    }

    @Override
    public String toString() {
        return "MembersView{" + "version=" + version + ", members=" + memberJoinVersions.keySet() + '}';
    }

}
