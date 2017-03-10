package com.hazelcast.internal.cluster.impl.operations;

import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.cluster.impl.MembersViewMetadata;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

import static com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook.TRIGGER_EXPLICIT_SUSPICION;

// TODO [basri] ADD JAVADOC
// TODO [basri] make this extend MembersUpdateOp, just like FinalizeJoinOp
public class TriggerExplicitSuspicionOp extends AbstractClusterOperation {

    private int callerMemberListVersion;

    private MembersViewMetadata suspectedMembersViewMetadata;

    public TriggerExplicitSuspicionOp() {
    }

    public TriggerExplicitSuspicionOp(int callerMemberListVersion, MembersViewMetadata suspectedMembersViewMetadata) {
        this.callerMemberListVersion = callerMemberListVersion;
        this.suspectedMembersViewMetadata = suspectedMembersViewMetadata;
    }

    @Override
    public void run() throws Exception {
        ClusterServiceImpl clusterService = getService();
        clusterService.handleExplicitSuspicionTrigger(getCallerAddress(), callerMemberListVersion, suspectedMembersViewMetadata);
    }

    @Override
    public int getId() {
        return TRIGGER_EXPLICIT_SUSPICION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(callerMemberListVersion);
        out.writeObject(suspectedMembersViewMetadata);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        callerMemberListVersion = in.readInt();
        suspectedMembersViewMetadata = in.readObject();
    }

}
