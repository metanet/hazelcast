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

import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.MemberVersion;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.util.UuidUtil.newUnsecureUuidString;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MemberMapTest {

    private static final MemberVersion VERSION = MemberVersion.of(BuildInfoProvider.getBuildInfo().getVersion());

    @Test(expected = IllegalArgumentException.class)
    public void testConstructor_whenAddressAndUuidMapsHaveDifferentMembers_thenThrowAssertionError() {
        Map<Address, MemberImpl> addressMap = new HashMap<Address, MemberImpl>();
        Map<String, MemberImpl> uuidMap = new HashMap<String, MemberImpl>();

        MemberImpl addressMember = newMember(5701);
        MemberImpl uuidMember = newMember(5702);

        addressMap.put(addressMember.getAddress(), addressMember);
        uuidMap.put(uuidMember.getUuid(), uuidMember);

        Map<MemberImpl, Integer> joinVersions = new LinkedHashMap<MemberImpl, Integer>();
        joinVersions.put(addressMember, 1);
        joinVersions.put(uuidMember, 2);

        new MemberMap(2, addressMap, uuidMap, joinVersions);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructor_whenAddressAndJoinVersionsMapsHaveDifferentMembers_thenThrowAssertionError() {
        Map<Address, MemberImpl> addressMap = new HashMap<Address, MemberImpl>();
        Map<String, MemberImpl> uuidMap = new HashMap<String, MemberImpl>();

        MemberImpl addressMember = newMember(5701);
        MemberImpl versionMember = newMember(5702);

        addressMap.put(addressMember.getAddress(), addressMember);
        uuidMap.put(addressMember.getUuid(), addressMember);

        Map<MemberImpl, Integer> joinVersions = new LinkedHashMap<MemberImpl, Integer>();
        joinVersions.put(versionMember, 1);

        new MemberMap(2, addressMap, uuidMap, joinVersions);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructor_whenJoinVersionsMapHaveDuplicateVersions_thenThrowAssertionError() {
        Map<Address, MemberImpl> addressMap = new HashMap<Address, MemberImpl>();
        Map<String, MemberImpl> uuidMap = new HashMap<String, MemberImpl>();

        MemberImpl member1 = newMember(5701);
        MemberImpl member2 = newMember(5702);

        addressMap.put(member1.getAddress(), member1);
        addressMap.put(member2.getAddress(), member2);
        uuidMap.put(member1.getUuid(), member1);
        uuidMap.put(member2.getUuid(), member2);

        Map<MemberImpl, Integer> joinVersions = new LinkedHashMap<MemberImpl, Integer>();
        joinVersions.put(member1, 1);
        joinVersions.put(member2, 1);

        new MemberMap(2, addressMap, uuidMap, joinVersions);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructor_whenVersionIsSmallerThanMemberCount_thenThrowAssertionError() {
        Map<Address, MemberImpl> addressMap = new HashMap<Address, MemberImpl>();
        Map<String, MemberImpl> uuidMap = new HashMap<String, MemberImpl>();

        MemberImpl member1 = newMember(5701);
        MemberImpl member2 = newMember(5702);

        addressMap.put(member1.getAddress(), member1);
        addressMap.put(member2.getAddress(), member2);
        uuidMap.put(member1.getUuid(), member1);
        uuidMap.put(member2.getUuid(), member2);

        Map<MemberImpl, Integer> joinVersions = new LinkedHashMap<MemberImpl, Integer>();
        joinVersions.put(member1, 1);
        joinVersions.put(member2, 2);

        int version = joinVersions.size() - 1;
        new MemberMap(version, addressMap, uuidMap, joinVersions);
    }

    @Test
    public void createEmpty() {
        MemberMap map = MemberMap.empty();
        assertTrue(map.getMembers().isEmpty());
        assertTrue(map.getAddresses().isEmpty());
        assertEquals(0, map.size());
    }

    @Test
    public void createSingleton() {
        MemberImpl member = newMember(5000);
        MemberMap map = MemberMap.singleton(member);
        assertEquals(1, map.getMembers().size());
        assertEquals(1, map.getAddresses().size());
        assertEquals(1, map.size());
        assertContains(map, member.getAddress());
        assertContains(map, member.getUuid());
        assertSame(member, map.getMember(member.getAddress()));
        assertSame(member, map.getMember(member.getUuid()));

        assertMemberSet(map);
    }

    @Test
    public void createNew() {
        MemberImpl[] members = newMembers(5);

        MemberMap map = MemberMap.createNew(members);
        assertEquals(members.length, map.getMembers().size());
        assertEquals(members.length, map.getAddresses().size());
        assertEquals(members.length, map.size());

        int version = 1;
        int idx = 0;
        for (MemberImpl member : members) {
            assertContains(map, member.getAddress());
            assertContains(map, member.getUuid());
            assertSame(member, map.getMember(member.getAddress()));
            assertSame(member, map.getMember(member.getUuid()));
            assertEquals(version, map.getMemberJoinVersion(member));
            assertEquals(idx, map.getMemberIndex(member));
            version++;
            idx++;
        }

        assertMemberSet(map);
    }

    @Test(expected = IllegalArgumentException.class)
    public void create_failsWithDuplicateAddress() {
        MemberImpl member1 = newMember(5000);
        MemberImpl member2 = newMember(5000);
        MemberMap.createNew(member1, member2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void create_failsWithDuplicateUuid() {
        MemberImpl member1 = newMember(5000);
        MemberImpl member2 = new MemberImpl(newAddress(5001), VERSION, false, member1.getUuid());
        MemberMap.createNew(member1, member2);
    }

    @Test
    public void cloneExcluding() {
        MemberImpl[] members = newMembers(6);

        MemberImpl exclude0 = members[0];
        MemberImpl exclude1 = new MemberImpl(newAddress(6000), VERSION, false, members[1].getUuid());
        MemberImpl exclude2 = new MemberImpl(members[2].getAddress(), VERSION, false, newUnsecureUuidString());

        MemberMap map = MemberMap.cloneExcluding(MemberMap.createNew(members), asList(exclude0, exclude1, exclude2));

        int numOfExcludedMembers = 3;
        assertEquals(members.length - numOfExcludedMembers, map.getMembers().size());
        assertEquals(members.length - numOfExcludedMembers, map.getAddresses().size());
        assertEquals(members.length - numOfExcludedMembers, map.size());

        for (int i = 0; i < numOfExcludedMembers; i++) {
            MemberImpl member = members[i];
            assertNotContains(map, member.getAddress());
            assertNotContains(map, member.getUuid());
            assertNull(map.getMember(member.getAddress()));
            assertNull(map.getMember(member.getUuid()));
        }

        for (int i = numOfExcludedMembers; i < members.length; i++) {
            MemberImpl member = members[i];
            assertContains(map, member.getAddress());
            assertContains(map, member.getUuid());
            assertSame(member, map.getMember(member.getAddress()));
            assertSame(member, map.getMember(member.getUuid()));
        }

        assertMemberSet(map);
    }

    @Test
    public void cloneExcluding_emptyMap() {
        MemberMap empty = MemberMap.empty();
        MemberMap map = MemberMap.cloneExcluding(empty, singleton(newMember(5000)));

        assertEquals(0, map.size());
        assertSame(empty, map);
    }

    @Test
    public void cloneAdding() {
        MemberImpl[] members = newMembers(5);

        MemberMap map = MemberMap.cloneAdding(MemberMap.createNew(members[0], members[1], members[2]), asList(members[3], members[4]));
        assertEquals(members.length, map.getMembers().size());
        assertEquals(members.length, map.getAddresses().size());
        assertEquals(members.length, map.size());
        assertEquals(4, map.getMemberJoinVersion(members[3]));
        assertEquals(5, map.getMemberJoinVersion(members[4]));

        for (MemberImpl member : members) {
            assertContains(map, member.getAddress());
            assertContains(map, member.getUuid());
            assertSame(member, map.getMember(member.getAddress()));
            assertSame(member, map.getMember(member.getUuid()));
        }

        assertMemberSet(map);
    }

    @Test(expected = IllegalArgumentException.class)
    public void cloneAdding_failsWithDuplicateAddress() {
        MemberImpl[] members = newMembers(3);

        MemberImpl member = newMember(5000);
        MemberMap.cloneAdding(MemberMap.createNew(members), singleton(member));
    }

    @Test(expected = IllegalArgumentException.class)
    public void cloneAdding_failsWithDuplicateUuid() {
        MemberImpl[] members = newMembers(3);

        MemberImpl member = new MemberImpl(newAddress(6000), VERSION, false, members[1].getUuid());
        MemberMap.cloneAdding(MemberMap.createNew(members), singleton(member));
    }

    @Test
    public void getMembers_ordered() {
        MemberImpl[] members = newMembers(10);

        MemberMap map = MemberMap.createNew(members);
        Set<MemberImpl> memberSet = map.getMembers();

        int index = 0;
        for (MemberImpl member : memberSet) {
            assertSame(members[index++], member);
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void getMembers_unmodifiable() {
        MemberImpl[] members = newMembers(5);
        MemberMap map = MemberMap.createNew(members);

        map.getMembers().add(newMember(9000));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void getAddresses_unmodifiable() {
        MemberImpl[] members = newMembers(5);
        MemberMap map = MemberMap.createNew(members);

        map.getAddresses().add(newAddress(9000));
    }

    @Test
    public void getMember_withAddressAndUuid_whenFound() {
        MemberImpl[] members = newMembers(3);
        MemberMap map = MemberMap.createNew(members);

        MemberImpl member = members[0];
        assertEquals(member, map.getMember(member.getAddress(), member.getUuid()));
    }

    @Test
    public void getMember_withAddressAndUuid_whenOnlyAddressFound() {
        MemberImpl[] members = newMembers(3);
        MemberMap map = MemberMap.createNew(members);

        assertNull(map.getMember(members[0].getAddress(), newUnsecureUuidString()));
    }

    @Test
    public void getMember_withAddressAndUuid_whenOnlyUuidFound() {
        MemberImpl[] members = newMembers(3);
        MemberMap map = MemberMap.createNew(members);

        assertNull(map.getMember(newAddress(6000), members[0].getUuid()));
    }

    @Test
    public void getMember_withAddressAndUuid_whenNotFound() {
        MemberImpl[] members = newMembers(3);
        MemberMap map = MemberMap.createNew(members);

        assertNull(map.getMember(newAddress(6000), newUnsecureUuidString()));
    }

    @Test
    public void tailMemberSet_inclusive() {
        MemberImpl[] members = newMembers(7);
        MemberMap map = MemberMap.createNew(members);

        MemberImpl member = members[3];
        Set<MemberImpl> set = map.tailMemberSet(member, true);

        assertEquals(4, set.size());
        int k = 3;
        for (MemberImpl m : set) {
            assertEquals(members[k++], m);
        }
    }

    @Test
    public void tailMemberSet_exclusive() {
        MemberImpl[] members = newMembers(7);
        MemberMap map = MemberMap.createNew(members);

        MemberImpl member = members[3];
        Set<MemberImpl> set = map.tailMemberSet(member, false);

        assertEquals(3, set.size());
        int k = 4;
        for (MemberImpl m : set) {
            assertEquals(members[k++], m);
        }
    }

    @Test
    public void headMemberSet_inclusive() {
        MemberImpl[] members = newMembers(7);
        MemberMap map = MemberMap.createNew(members);

        MemberImpl member = members[3];
        Set<MemberImpl> set = map.headMemberSet(member, true);

        assertEquals(4, set.size());
        int k = 0;
        for (MemberImpl m : set) {
            assertEquals(members[k++], m);
        }
    }

    @Test
    public void headMemberSet_exclusive() {
        MemberImpl[] members = newMembers(7);
        MemberMap map = MemberMap.createNew(members);

        MemberImpl member = members[3];
        Set<MemberImpl> set = map.headMemberSet(member, false);

        assertEquals(3, set.size());
        int k = 0;
        for (MemberImpl m : set) {
            assertEquals(members[k++], m);
        }
    }

    @Test
    public void isBeforeThan_success() {
        MemberImpl[] members = newMembers(5);
        MemberMap map = MemberMap.createNew(members);

        assertTrue(map.isBeforeThan(members[1].getAddress(), members[3].getAddress()));
    }

    @Test
    public void isBeforeThan_fail() {
        MemberImpl[] members = newMembers(5);
        MemberMap map = MemberMap.createNew(members);

        assertFalse(map.isBeforeThan(members[4].getAddress(), members[1].getAddress()));
    }

    @Test
    public void isBeforeThan_whenFirstAddressNotExist() {
        MemberImpl[] members = newMembers(5);
        MemberMap map = MemberMap.createNew(members);

        map.isBeforeThan(newAddress(6000), members[0].getAddress());
    }

    @Test
    public void isBeforeThan_whenSecondAddressNotExist() {
        MemberImpl[] members = newMembers(5);
        MemberMap map = MemberMap.createNew(members);

        map.isBeforeThan(members[0].getAddress(), newAddress(6000));
    }

    @Test
    public void isBeforeThan_whenAddressesNotExist() {
        MemberImpl[] members = newMembers(5);
        MemberMap map = MemberMap.createNew(members);

        map.isBeforeThan(newAddress(6000), newAddress(7000));
    }

    static MemberImpl[] newMembers(int count) {
        MemberImpl[] members = new MemberImpl[count];
        for (int i = 0; i < members.length; i++) {
            members[i] = newMember(5000 + i);
        }
        return members;
    }

    static MemberImpl newMember(int port) {
        return new MemberImpl(newAddress(port), VERSION, false, newUnsecureUuidString());
    }

    private static Address newAddress(int port) {
        try {
            return new Address(InetAddress.getLocalHost(), port);
        } catch (UnknownHostException e) {
            fail("Could not create new Address: " + e.getMessage());
        }
        return null;
    }

    private static void assertMemberSet(MemberMap map) {
        for (MemberImpl member : map.getMembers()) {
            assertContains(map, member.getAddress());
            assertContains(map, member.getUuid());
            assertEquals(member, map.getMember(member.getAddress()));
            assertEquals(member, map.getMember(member.getUuid()));
        }
    }

    private static void assertContains(MemberMap map, Address address) {
        assertTrue("MemberMap doesn't contain expected " + address, map.contains(address));
    }

    private static void assertContains(MemberMap map, String uuid) {
        assertTrue("MemberMap doesn't contain expected " + uuid, map.contains(uuid));
    }

    private static void assertNotContains(MemberMap map, Address address) {
        assertFalse("MemberMap contains unexpected " + address, map.contains(address));
    }

    private static void assertNotContains(MemberMap map, String uuid) {
        assertFalse("MemberMap contains unexpected " + uuid, map.contains(uuid));
    }
}
