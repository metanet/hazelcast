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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableList;

/**
 * TODO
 */
public final class MembersView implements IdentifiedDataSerializable {

    private int version;
    private List<MemberInfo> members;

    public MembersView() {
    }

    public MembersView(int version, List<MemberInfo> members) {
        this.version = version;
        this.members = members;
    }

    /**
     * Creates an empty {@code MemberMap}.
     *
     * @return empty {@code MemberMap}
     */
    static MembersView empty() {
        return new MembersView(0, Collections.<MemberInfo>emptyList());
    }

    /**
     * Creates a singleton {@code MemberMap} including only specified member.
     *
     * @param member sole member in map
     * @return singleton {@code MemberMap}
     */
    static MembersView singleton(MemberImpl member) {
        return new MembersView(1, singletonList(new MemberInfo(member)));
    }

//    /**
//     * Creates clone of source {@link MembersView} additionally including new members.
//     *
//     * @param source     source map
//     * @param newMembers new members to add
//     * @return clone map
//     */
//    static MembersView cloneAdding(MembersView source, MemberImpl... newMembers) {
//        List<MemberInfo> list = new ArrayList<MemberInfo>(source.size() + newMembers.length);
//        list.addAll(source.getMembers());
//
//        for (MemberImpl member : newMembers) {
//            list.add(new MemberInfo(member));
//        }
//        return new MembersView(source.version + 1, list);
//    }

    /**
     * Creates clone of source {@link MembersView} additionally including new members.
     *
     * @param source     source map
     * @param newMembers new members to add
     * @return clone map
     */
    static MembersView cloneAdding(MembersView source, Collection<MemberInfo> newMembers) {
        List<MemberInfo> list = new ArrayList<MemberInfo>(source.size() + newMembers.size());
        list.addAll(source.getMembers());
        list.addAll(newMembers);

        return new MembersView(source.version + 1, unmodifiableList(list));
    }

    /**
     * Creates a new {@code MemberMap} including given members.
     *
     * @param members members
     * @return a new {@code MemberMap}
     */
    static MembersView createNew(Collection<MemberImpl> members) {
        return createNew(0, members);
    }

    /**
     * Creates a new {@code MemberMap} including given members.
     *
     * @param version version
     * @param members members
     * @return a new {@code MemberMap}
     */
    public static MembersView createNew(int version, Collection<MemberImpl> members) {
        List<MemberInfo> list = new ArrayList<MemberInfo>(members.size());

        for (MemberImpl member : members) {
            list.add(new MemberInfo(member));
        }

        return new MembersView(version, unmodifiableList(list));
    }

    public List<MemberInfo> getMembers() {
        return members;
    }

    public int size() {
        return members.size();
    }

    public int getVersion() {
        return version;
    }

    MemberMap toMemberMap() {
        MemberImpl[] m = new MemberImpl[size()];
        int ix = 0;
        for (MemberInfo memberInfo : members) {
            m[ix++] = memberInfo.toMember();
        }
        return MemberMap.createNew(version, m);
    }

    public boolean containsAddress(Address address) {
        for (MemberInfo member : members) {
            if (member.getAddress().equals(address)) {
                return true;
            }
        }

        return false;
    }

    public boolean containsAddress(Address address, String uuid) {
        for (MemberInfo member : members) {
            if (member.getAddress().equals(address)) {
                return member.getUuid().equals(uuid);
            }
        }

        return false;
    }

    public Set<Address> getAddresses() {
        Set<Address> addresses = new HashSet<Address>(members.size());
        for (MemberInfo member : members) {
            addresses.add(member.getAddress());
        }
        return addresses;
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
        out.writeInt(members.size());
        for (MemberInfo member : members) {
            member.writeData(out);
        }
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        version = in.readInt();
        int size = in.readInt();
        List<MemberInfo> members = new ArrayList<MemberInfo>(size);
        for (int i = 0; i < size; i++) {
            MemberInfo member = new MemberInfo();
            member.readData(in);
            members.add(member);
        }

        this.members = unmodifiableList(members);
    }

    @Override
    public String toString() {
        return "MembersView{" + "version=" + version + ", members=" + members + '}';
    }

}
