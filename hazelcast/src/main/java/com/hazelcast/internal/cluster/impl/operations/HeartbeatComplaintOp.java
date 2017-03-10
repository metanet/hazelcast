package com.hazelcast.internal.cluster.impl.operations;

import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.cluster.impl.MembersViewMetadata;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

import static com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook.HEARTBEAT_COMPLAINT;

public class HeartbeatComplaintOp extends AbstractClusterOperation {

    private MembersViewMetadata receiverMembersViewMetadata;

    private MembersViewMetadata senderMembersViewMetadata;

    public HeartbeatComplaintOp() {
    }

    public HeartbeatComplaintOp(MembersViewMetadata receiverMembersViewMetadata, MembersViewMetadata senderMembersViewMetadata) {
        this.receiverMembersViewMetadata = receiverMembersViewMetadata;
        this.senderMembersViewMetadata = senderMembersViewMetadata;
    }

    @Override
    public void run() throws Exception {
        ClusterServiceImpl service = getService();
        service.handleHeartbeatComplaint(receiverMembersViewMetadata, senderMembersViewMetadata);
    }

    @Override
    public int getId() {
        return HEARTBEAT_COMPLAINT;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
        out.writeObject(receiverMembersViewMetadata);
        out.writeObject(senderMembersViewMetadata);
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        receiverMembersViewMetadata = in.readObject();
        senderMembersViewMetadata = in.readObject();
    }

}
