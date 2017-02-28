/*
 * Copyright (c) 2008 - 2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.instance.TestUtil;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.cluster.impl.MembershipFailureTest.assertMaster;
import static com.hazelcast.internal.cluster.impl.MembershipUpdateTest.assertMemberViewsAreSame;
import static com.hazelcast.internal.cluster.impl.MembershipUpdateTest.getMemberMap;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class MembershipFailureTest_withTCP extends HazelcastTestSupport {

    @After
    public void tearDown() {
        HazelcastInstanceFactory.terminateAll();
    }

    @Test
    public void master_detects_slave_failure() {
        HazelcastInstance master = Hazelcast.newHazelcastInstance(newConfig());
        HazelcastInstance slave = Hazelcast.newHazelcastInstance(newConfig());

        assertClusterSizeEventually(2, master);
        assertClusterSizeEventually(2, slave);

        TestUtil.terminateInstance(slave);

        assertClusterSizeEventually(1, master);
    }

    @Test
    public void slaves_detect_master_failure() {
        HazelcastInstance master = Hazelcast.newHazelcastInstance(newConfig());
        HazelcastInstance slave1 = Hazelcast.newHazelcastInstance(newConfig());
        HazelcastInstance slave2 = Hazelcast.newHazelcastInstance(newConfig());

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

    private static Config newConfig() {
        Config config = new Config();
        config.setProperty(GroupProperty.WAIT_SECONDS_BEFORE_JOIN.getName(), "0");
        
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);

        TcpIpConfig tcpIpConfig = join.getTcpIpConfig().setEnabled(true).clear();
        for (int i = 0; i < 4; i++) {
            int port = 5701 + i;
            tcpIpConfig.addMember("127.0.0.1:" + port);
        }

        return config;
    }
}
