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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.TestUtil;
import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.cluster.impl.MembershipUpdateTest.assertMemberViewsAreSame;
import static com.hazelcast.internal.cluster.impl.MembershipUpdateTest.getMemberMap;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MembershipFailureTest extends HazelcastTestSupport {

    private TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

    // TODO: add membership failure tests
    // - slave member failure detected by master
    // - slave member suspected by others and its failure eventually detected by master
    // - master member failure detected by others
    // - master and master-candidate fail simultaneously
    // - master fails when master-candidate doesn't have the most recent member list
    // - partial network failure: multiple master claims, eventually split brain and merge
    // - so on...

    @Test
    public void master_detects_slave_failure() {
        HazelcastInstance master = factory.newHazelcastInstance();
        HazelcastInstance slave = factory.newHazelcastInstance();

        assertClusterSizeEventually(2, master);
        assertClusterSizeEventually(2, slave);

        TestUtil.terminateInstance(slave);

        assertClusterSizeEventually(1, master);
    }

    @Test
    public void slaves_detect_master_failure() {
        HazelcastInstance master = factory.newHazelcastInstance();
        HazelcastInstance slave1 = factory.newHazelcastInstance();
        HazelcastInstance slave2 = factory.newHazelcastInstance();

        assertClusterSizeEventually(3, master);
        assertClusterSizeEventually(3, slave1);
        assertClusterSizeEventually(3, slave2);

        TestUtil.terminateInstance(master);

        assertClusterSizeEventually(2, slave1);
        assertClusterSizeEventually(2, slave2);

        assertMaster(slave1, getAddress(slave1));
        assertMaster(slave2, getAddress(slave1));

        MemberMap memberMap1 = getMemberMap(slave1);
        MemberMap memberMap2 = getMemberMap(slave2);
        assertMemberViewsAreSame(memberMap1, memberMap2);
    }

    static void assertMaster(HazelcastInstance instance, Address address) {
        assertEquals(address, getNode(instance).getMasterAddress());
    }
}
