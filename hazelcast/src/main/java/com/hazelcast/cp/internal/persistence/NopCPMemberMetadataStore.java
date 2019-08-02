/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.persistence;

import com.hazelcast.cp.CPMember;
import com.hazelcast.nio.Address;

/**
 * Used when CP Subsystem works transiently (its state is not persisted).
 */
public final class NopCPMemberMetadataStore implements CPMemberMetadataStore {

    public static final CPMemberMetadataStore INSTANCE = new NopCPMemberMetadataStore();

    private NopCPMemberMetadataStore() {
    }

    @Override
    public boolean isMarkedAPMember() {
        return false;
    }

    @Override
    public boolean tryMarkAPMember() {
        return false;
    }

    @Override
    public void persistLocalMember(CPMember member) {
    }

    @Override
    public CPMember readLocalMember(Address address) {
        return null;
    }

}
