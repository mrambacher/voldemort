package voldemort.client.protocol.vold;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import voldemort.VoldemortException;
import voldemort.client.protocol.RequestFormatType;
import voldemort.protocol.vold.VoldemortNativeProtocol;
import voldemort.serialization.VoldemortOpCode;
import voldemort.server.RequestRoutingType;
import voldemort.store.InsufficientOperationalNodesException;
import voldemort.store.InsufficientSuccessfulNodesException;
import voldemort.store.StoreUtils;
import voldemort.utils.ByteArray;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.VectorClockProtoSerializer;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

public class VoldemortNativeClientRequestFormatV3 extends VoldemortNativeClientRequestFormatV2 {

    @Override
    protected RequestFormatType getProtocol() {
        return RequestFormatType.VOLDEMORT_V3;
    }

    @Override
    public void writeDeleteRequest(DataOutputStream outputStream,
                                   String storeName,
                                   ByteArray key,
                                   Version version,
                                   RequestRoutingType routingType) throws IOException {
        StoreUtils.assertValidKey(key);
        writeMessageHeader(outputStream, VoldemortOpCode.DELETE_OP_CODE, storeName, routingType);
        VoldemortNativeProtocol.writeKey(outputStream, key);

        writeVersion(outputStream, version);
    }

    @Override
    protected void writeVersion(DataOutputStream outputStream, Version version) throws IOException {
        byte[] bytes = VectorClockProtoSerializer.toBytes(version);
        outputStream.writeInt(bytes.length);
        outputStream.write(bytes);
    }

    @Override
    protected void writeVersioned(DataOutputStream outputStream, Versioned<byte[]> versioned)
            throws IOException {
        byte[] bytes = VectorClockProtoSerializer.toBytes(versioned);
        outputStream.writeInt(bytes.length);
        outputStream.write(bytes);
    }

    @Override
    public Version readPutResponse(DataInputStream inputStream) throws IOException {
        checkException(inputStream);
        return VoldemortNativeProtocol.readVersion(inputStream);
    }

    @Override
    protected VoldemortException getException(short code, DataInputStream inputStream)
            throws IOException {
        VoldemortException ex = super.getException(code, inputStream);
        if(ex instanceof ObsoleteVersionException) {
            ObsoleteVersionException ove = (ObsoleteVersionException) ex;
            Version version = VoldemortNativeProtocol.readVersion(inputStream);
            if(version != null) {
                ove.setExistingVersion(version);
            }
        } else if(ex instanceof InsufficientSuccessfulNodesException) {
            InsufficientSuccessfulNodesException isne = (InsufficientSuccessfulNodesException) ex;
            isne.setAvailable(inputStream.readInt());
            isne.setRequired(inputStream.readInt());
            isne.setSuccessful(inputStream.readInt());
        } else if(ex instanceof InsufficientOperationalNodesException) {
            InsufficientOperationalNodesException ione = (InsufficientOperationalNodesException) ex;
            ione.setAvailable(inputStream.readInt());
            ione.setRequired(inputStream.readInt());
        }
        return ex;
    }
}
