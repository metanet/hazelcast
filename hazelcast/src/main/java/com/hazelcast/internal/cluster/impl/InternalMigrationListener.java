package com.hazelcast.internal.cluster.impl;

import com.hazelcast.partition.MigrationInfo;

public interface InternalMigrationListener {

    enum MigrationParticipant {
        MASTER,
        SOURCE,
        TARGET
    }

    void onMigrationStart(MigrationParticipant participant, MigrationInfo migrationInfo);

    void onMigrationComplete(MigrationParticipant participant, MigrationInfo migrationInfo, boolean success);

    void onMigrationCommit(MigrationParticipant participant, MigrationInfo migrationInfo);

    void onMigrationRollback(MigrationParticipant participant, MigrationInfo migrationInfo);

}
