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

import com.hazelcast.core.Member;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.Address;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static com.hazelcast.util.Preconditions.checkFalse;
import static com.hazelcast.util.Preconditions.checkTrue;
import static java.util.Collections.singletonMap;
import static java.util.Collections.sort;
import static java.util.Collections.unmodifiableMap;

/**
 * A special, immutable {@link MemberImpl} map type,
 * that allows querying members using address or uuid.
 */
final class MemberMap {

    private final int version;
    private final Map<Address, MemberImpl> addressToMemberMap;
    private final Map<String, MemberImpl> uuidToMemberMap;
    private final Map<MemberImpl, Integer> memberToJoinVersionMap;

    /**
     * Assumes that addressMap, uuidMap, and memberToJoinVersionMap to be sorted by join order.
     */
    MemberMap(int version,
              Map<Address, MemberImpl> addressMap,
              Map<String, MemberImpl> uuidMap,
              Map<MemberImpl, Integer> memberToJoinVersionMap) {
        checkTrue(new HashSet<MemberImpl>(addressMap.values()).equals(new HashSet<MemberImpl>(uuidMap.values())),
                "Maps are different! AddressMap: " + addressMap + ", UuidMap: " + uuidMap);
        checkTrue(new HashSet<MemberImpl>(addressMap.values()).equals(new HashSet<MemberImpl>(memberToJoinVersionMap.keySet())),
                "Maps are different! AddressMap: " + addressMap + ", JoinVersionMap: " + memberToJoinVersionMap);
        checkTrue(addressMap.size() == new HashSet<Integer>(memberToJoinVersionMap.values()).size(),
                "JoinVersionMap: " + memberToJoinVersionMap + " has duplicate join versions!");
        for (Entry<MemberImpl, Integer> e : memberToJoinVersionMap.entrySet()) {
            checkFalse(version < e.getValue(), "Version: " + version
                    + " cannot be smaller than join version: " + e.getValue() + " of " + e.getKey());
        }
        this.version = version;
        this.addressToMemberMap = unmodifiableMap(addressMap);
        this.uuidToMemberMap = unmodifiableMap(uuidMap);
        this.memberToJoinVersionMap = unmodifiableMap(memberToJoinVersionMap);
    }

    /**
     * Creates an empty {@code MemberMap} with version number of 0.
     *
     * @return empty {@code MemberMap}
     */
    static MemberMap empty() {
        return new MemberMap(0, Collections.<Address, MemberImpl>emptyMap(), Collections.<String, MemberImpl>emptyMap(),
                Collections.<MemberImpl, Integer>emptyMap());
    }

    /**
     * Creates a singleton {@code MemberMap} with version number of 1 and including only the specified member.
     *
     * @param member sole member in map
     * @return singleton {@code MemberMap}
     */
    static MemberMap singleton(MemberImpl member) {
        return new MemberMap(1, singletonMap(member.getAddress(), member), singletonMap(member.getUuid(), member),
                singletonMap(member, 1));
    }

    // membersRemovedInNotJoinable create ediyor
    /**
     * Creates a new {@code MemberMap} including the given members with version number equal to the member count
     * Each member is assigned a join version, starting from 1. The member list version number is equal to the member count.
     *
     * @param members members
     * @return a new {@code MemberMap}
     */
    static MemberMap createNew(MemberImpl... members) {
        Map<MemberImpl, Integer> joinVersionMap = new HashMap<MemberImpl, Integer>();
        int version = 0;
        for (MemberImpl member : members) {
            joinVersionMap.put(member, ++version);
        }
        return createNew(version, joinVersionMap);
    }

    // buna joinVersions koyacagiz.
    // ikinci parametreyi Map<MemberImpl, Integer> yap.
    /**
     * Creates a new {@code MemberMap} including the given members.
     * Each member must have a unique join version which is smaller than or equal to the member list version number.
     *
     * @param version version
     * @param memberJoinVersions members along with their join versions. Members can be given in any order.
     * @return a new {@code MemberMap}
     */
    static MemberMap createNew(int version, Map<MemberImpl, Integer> memberJoinVersions) {
        Map<Address, MemberImpl> addressMap = new LinkedHashMap<Address, MemberImpl>();
        Map<String, MemberImpl> uuidMap = new LinkedHashMap<String, MemberImpl>();
        Map<MemberImpl, Integer> joinVersionMap = new LinkedHashMap<MemberImpl, Integer>();

        for (MemberImpl member : sortMembersByJoinOrder(memberJoinVersions)) {
            putMember(addressMap, uuidMap, joinVersionMap, member, memberJoinVersions.get(member));
        }

        return new MemberMap(version, addressMap, uuidMap, joinVersionMap);
    }

    // updateMembers()'ta membersRemovedInNotJoinable ve removeMember()'da members guncelleniyor
    /**
     * Creates clone of source {@code MemberMap}, excluding given members.
     * If source is empty, same map instance will be returned. If excluded members are empty or not present in
     * source, a new map will be created with incremented version number, containing the same members with source.
     *
     * @param source         source map
     * @param excludeMembers members to exclude
     * @return clone map
     */
    static MemberMap cloneExcluding(MemberMap source, Collection<MemberImpl> excludeMembers) {
        if (source.size() == 0) {
            return source;
        }

        Map<Address, MemberImpl> addressMap = new LinkedHashMap<Address, MemberImpl>(source.addressToMemberMap);
        Map<String, MemberImpl> uuidMap = new LinkedHashMap<String, MemberImpl>(source.uuidToMemberMap);
        Map<MemberImpl, Integer> joinVersionsMap = new LinkedHashMap<MemberImpl, Integer>(source.memberToJoinVersionMap);

        for (MemberImpl member : excludeMembers) {
            MemberImpl removed = addressMap.remove(member.getAddress());
            if (removed != null) {
                uuidMap.remove(removed.getUuid());
                joinVersionsMap.remove(removed);
            }

            removed = uuidMap.remove(member.getUuid());
            if (removed != null) {
                addressMap.remove(removed.getAddress());
                joinVersionsMap.remove(removed);
            }
        }

        return new MemberMap(source.version + 1, addressMap, uuidMap, joinVersionsMap);
    }

    // bu non-versioned
    // handleMemberRemove()'da membersRemovedInNotJoinable guncelleniyor, addMembersRemovedInNotJoinable()'ta guncelleniyor
    /**
     * Creates clone of source {@code MemberMap} additionally including new members.
     * The version number is incremented for each new member.
     *
     * @param source     source map
     * @param newMembers new members to add
     * @return clone map
     */
    static MemberMap cloneAdding(MemberMap source, Collection<MemberImpl> newMembers) {
        Map<Address, MemberImpl> addressMap = new LinkedHashMap<Address, MemberImpl>(source.addressToMemberMap);
        Map<String, MemberImpl> uuidMap = new LinkedHashMap<String, MemberImpl>(source.uuidToMemberMap);
        Map<MemberImpl, Integer> joinVersionMap = new LinkedHashMap<MemberImpl, Integer>(source.memberToJoinVersionMap);

        int newVersion = source.version;
        for (MemberImpl member : newMembers) {
            putMember(addressMap, uuidMap, joinVersionMap, member, ++newVersion);
        }

        return new MemberMap(newVersion, addressMap, uuidMap, joinVersionMap);
    }

    private static Set<MemberImpl> sortMembersByJoinOrder(final Map<MemberImpl, Integer> memberJoinVersions) {
        List<MemberImpl> members = new ArrayList<MemberImpl>(memberJoinVersions.keySet());
        sort(members, new Comparator<MemberImpl>() {
            @Override
            public int compare(MemberImpl m1, MemberImpl m2) {
                return memberJoinVersions.get(m1).compareTo(memberJoinVersions.get(m2));
            }
        });
        return new LinkedHashSet<MemberImpl>(members);
    }

    private static void putMember(Map<Address, MemberImpl> addressMap,
                                  Map<String, MemberImpl> uuidMap,
                                  Map<MemberImpl, Integer> joinVersionMap,
                                  MemberImpl member, int joinVersion) {

        MemberImpl current = addressMap.put(member.getAddress(), member);
        if (current != null) {
            throw new IllegalArgumentException("Replacing existing member with address: " + member);
        }

        current = uuidMap.put(member.getUuid(), member);
        if (current != null) {
            throw new IllegalArgumentException("Replacing existing member with uuid: " + member);
        }

        Integer currentVersion = joinVersionMap.put(member, joinVersion);
        if (currentVersion != null) {
            throw new IllegalArgumentException("Replacing existing member: " + member + " with join version: " + joinVersion);
        }
    }

    MemberImpl getMember(Address address) {
        return addressToMemberMap.get(address);
    }

    MemberImpl getMember(String uuid) {
        return uuidToMemberMap.get(uuid);
    }

    MemberImpl getMember(Address address, String uuid) {
        MemberImpl member1 = addressToMemberMap.get(address);
        MemberImpl member2 = uuidToMemberMap.get(uuid);

        if (member1 != null && member2 != null && member1.equals(member2)) {
            return member1;
        }
        return null;
    }

    public int getMemberIndex(MemberImpl member) {
        int i = 0;
        for (MemberImpl m : memberToJoinVersionMap.keySet()) {
            if (m.equals(member)) {
                return i;
            }
            i++;
        }

        throw new IllegalArgumentException(member + " is not present in " + memberToJoinVersionMap.keySet());
    }

    boolean contains(Address address) {
        return addressToMemberMap.containsKey(address);
    }

    boolean contains(String uuid) {
        return uuidToMemberMap.containsKey(uuid);
    }

    Set<MemberImpl> getMembers() {
        return memberToJoinVersionMap.keySet();
    }

    Collection<Address> getAddresses() {
        return addressToMemberMap.keySet();
    }

    int getMemberJoinVersion(MemberImpl member) {
        return memberToJoinVersionMap.get(member);
    }

    int size() {
        return memberToJoinVersionMap.size();
    }

    int getVersion() {
        return version;
    }

    MembersView toMembersView() {
        return MembersView.createNew(version, memberToJoinVersionMap);
    }

    MembersView toTailMembersView(MemberImpl member, boolean inclusive) {
        Map<MemberImpl, Integer> members = new LinkedHashMap<MemberImpl, Integer>();
        for (MemberImpl m : tailMemberSet(member, inclusive)) {
            members.put(m, memberToJoinVersionMap.get(m));
        }

        return MembersView.createNew(version, members);
    }

    Set<MemberImpl> tailMemberSet(MemberImpl member, boolean inclusive) {
        ensureMemberExist(member);

        Set<MemberImpl> result = new LinkedHashSet<MemberImpl>();
        boolean found = false;
        for (MemberImpl m : getMembers()) {
            if (!found && m.equals(member)) {
                found = true;
                if (inclusive) {
                    result.add(m);
                }
                continue;
            }

            if (found) {
                result.add(m);
            }
        }

        assert found : member + " should have been found!";

        return result;
    }

    Set<MemberImpl> headMemberSet(Member member, boolean inclusive) {
        ensureMemberExist(member);

        Set<MemberImpl> result = new LinkedHashSet<MemberImpl>();
        for (MemberImpl m : getMembers()) {
            if (!m.equals(member)) {
                result.add(m);
                continue;
            }

            if (inclusive) {
                result.add(m);
            }
            break;
        }

        return result;
    }

    boolean isBeforeThan(Address address1, Address address2) {
        if (address1.equals(address2)) {
            return false;
        }

        if (!addressToMemberMap.containsKey(address1)) {
            return false;
        }

        if (!addressToMemberMap.containsKey(address2)) {
            return false;
        }

        for (MemberImpl member : getMembers()) {
            if (member.getAddress().equals(address1)) {
                return true;
            }
            if (member.getAddress().equals(address2)) {
                return false;
            }
        }

        throw new AssertionError("Unreachable!");
    }

    private void ensureMemberExist(Member member) {
        if (!addressToMemberMap.containsKey(member.getAddress())) {
            throw new IllegalArgumentException(member + " not found!");
        }
        if (!uuidToMemberMap.containsKey(member.getUuid())) {
            throw new IllegalArgumentException(member + " not found!");
        }
    }

}
