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
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;

public class VoldemortNativeClientRequestFormatV3 extends VoldemortNativeClientRequestFormatV2 {

    @Override
    protected RequestFormatType getProtocol() {
        return RequestFormatType.VOLDEMORT_V3;
    }

    @Override
    public void writeDeleteRequest(DataOutputStream outputStream,
                                   String storeName,
                                   ByteArray key,
                                   VectorClock version,
                                   RequestRoutingType routingType) throws IOException {
        StoreUtils.assertValidKey(key);
        writeMessageHeader(outputStream, VoldemortOpCode.DELETE_OP_CODE, storeName, routingType);
        VoldemortNativeProtocol.writeKey(outputStream, key);

        VectorClock clock = version;
        VoldemortNativeProtocol.writeVersion(outputStream, clock);
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
