package com.hazelcast.internal.cluster.impl;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

public class MembersViewMetadata implements IdentifiedDataSerializable {

    private Address memberAddress;

    private String memberUuid;

    private Address masterAddress;

    private int memberListVersion;

    public MembersViewMetadata() {
    }

    public MembersViewMetadata(Address memberAddress, String memberUuid, Address masterAddress, int memberListVersion) {
        this.memberAddress = memberAddress;
        this.memberUuid = memberUuid;
        this.masterAddress = masterAddress;
        this.memberListVersion = memberListVersion;
    }

    public Address getMemberAddress() {
        return memberAddress;
    }

    public String getMemberUuid() {
        return memberUuid;
    }

    public Address getMasterAddress() {
        return masterAddress;
    }

    public int getMemberListVersion() {
        return memberListVersion;
    }

    @Override
    public int getFactoryId() {
        return ClusterDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return ClusterDataSerializerHook.MEMBERS_VIEW_METADATA;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(memberAddress);
        out.writeUTF(memberUuid);
        out.writeObject(masterAddress);
        out.writeInt(memberListVersion);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        memberAddress = in.readObject();
        memberUuid = in.readUTF();
        masterAddress = in.readObject();
        memberListVersion = in.readInt();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MembersViewMetadata that = (MembersViewMetadata) o;

        if (memberListVersion != that.memberListVersion) {
            return false;
        }
        if (!memberAddress.equals(that.memberAddress)) {
            return false;
        }
        if (!memberUuid.equals(that.memberUuid)) {
            return false;
        }
        return masterAddress.equals(that.masterAddress);
    }

    @Override
    public int hashCode() {
        int result = memberAddress.hashCode();
        result = 31 * result + memberUuid.hashCode();
        result = 31 * result + masterAddress.hashCode();
        result = 31 * result + memberListVersion;
        return result;
    }

    @Override
    public String toString() {
        return "MembersViewMetadata{" + "memberAddress=" + memberAddress + ", memberUuid='" + memberUuid + '\''
                + ", masterAddress=" + masterAddress + ", memberListVersion=" + memberListVersion + '}';
    }

}
