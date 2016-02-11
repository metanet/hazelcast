/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
 *
 */

package com.hazelcast.partition.impl;

import com.hazelcast.partition.MigrationInfo;

import java.util.EventListener;

public interface InternalMigrationListener extends EventListener {

    enum MigrationParticipant {
        MASTER,
        SOURCE,
        DESTINATION
    }

    void onMigrationStart(MigrationParticipant participant, MigrationInfo migrationInfo);

    void onMigrationComplete(MigrationParticipant participant, MigrationInfo migrationInfo, boolean success);

    void onMigrationCommit(MigrationParticipant participant, MigrationInfo migrationInfo);

    void onMigrationRollback(MigrationParticipant participant, MigrationInfo migrationInfo);


    class InternalMigrationListenerAdaptor implements InternalMigrationListener {
        @Override
        public void onMigrationStart(MigrationParticipant participant, MigrationInfo migrationInfo) {
        }

        @Override
        public void onMigrationComplete(MigrationParticipant participant, MigrationInfo migrationInfo,
                boolean success) {
        }

        @Override
        public void onMigrationCommit(MigrationParticipant participant, MigrationInfo migrationInfo) {
        }

        @Override
        public void onMigrationRollback(MigrationParticipant participant, MigrationInfo migrationInfo) {
        }
    }

}
