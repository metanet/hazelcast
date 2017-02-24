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

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MembershipUpdateTest extends HazelcastTestSupport {

    // TODO: add membership update tests
    // - sequential member join
    // - concurrent member join
    // - sequential member join and removal
    // - concurrent member join and removal
    // - existing members missing member updates (join), convergence
    // - existing member missing member removal, then receives periodic member publish
    // - existing member missing member removal, then receives new member join update
    // - existing member receiving out-of-order member updates
    // - new member receiving out-of-order finalize join & member updates
    // - existing member receiving a member list that's not containing itself
    // - new member receiving a finalize join that's not containing itself
    // - byzantine member updates published
    // - so on...
    
}
