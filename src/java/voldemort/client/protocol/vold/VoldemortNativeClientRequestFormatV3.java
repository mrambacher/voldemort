package voldemort.client.protocol.vold;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

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
    protected int getHeaderSize(byte operation, String storeName, RequestRoutingType routingType)
            throws IOException {
        // 1 byte for VoldemortOpCode
        // N bytes for store name
        // 1 byte for RequestRoutingType
        return 1 + VoldemortNativeProtocol.getStringRequestSize(storeName) + 1;
    }

    @Override
    public void writeGetVersionRequest(DataOutputStream outputStream,
                                       String storeName,
                                       ByteArray key,
                                       RequestRoutingType routingType) throws IOException {
        int requestSize = getHeaderSize(VoldemortOpCode.GET_OP_CODE, storeName, routingType)
                          + VoldemortNativeProtocol.getKeyRequestSize(key);

        outputStream.writeInt(requestSize);
        super.writeGetVersionRequest(outputStream, storeName, key, routingType);
    }

    @Override
    public void writeDeleteRequest(DataOutputStream outputStream,
                                   String storeName,
                                   ByteArray key,
                                   Version version,
                                   RequestRoutingType routingType) throws IOException {
        StoreUtils.assertValidKey(key);
        int requestSize = getHeaderSize(VoldemortOpCode.GET_OP_CODE, storeName, routingType)
                          + VoldemortNativeProtocol.getKeyRequestSize(key)
                          + this.getVersionSize(version);

        outputStream.writeInt(requestSize);

        writeMessageHeader(outputStream, VoldemortOpCode.DELETE_OP_CODE, storeName, routingType);

        VoldemortNativeProtocol.writeKey(outputStream, key);
        writeVersion(outputStream, version);
    }

    @Override
    protected void writeGetAllRequest(DataOutputStream output,
                                      String storeName,
                                      List<ByteArray> keys,
                                      RequestRoutingType routingType) throws IOException {
        StoreUtils.assertValidKeys(keys);
        int requestSize = getHeaderSize(VoldemortOpCode.GET_ALL_OP_CODE, storeName, routingType) + 4; // Header
        // +
        // list
        // size
        for(ByteArray key: keys) {
            requestSize += VoldemortNativeProtocol.getKeyRequestSize(key);
        }
        System.out.println("GETALL SIZE: " + requestSize);
        output.writeInt(requestSize);
        super.writeGetAllRequest(output, storeName, keys, routingType);
    }

    @Override
    public void writeGetRequest(DataOutputStream outputStream,
                                String storeName,
                                ByteArray key,
                                RequestRoutingType routingType) throws IOException {
        StoreUtils.assertValidKey(key);
        int requestSize = getHeaderSize(VoldemortOpCode.GET_OP_CODE, storeName, routingType)
                          + VoldemortNativeProtocol.getKeyRequestSize(key);
        outputStream.writeInt(requestSize);
        super.writeGetRequest(outputStream, storeName, key, routingType);
    }

    @Override
    public void writePutRequest(DataOutputStream outputStream,
                                String storeName,
                                ByteArray key,
                                Versioned<byte[]> versioned,
                                RequestRoutingType routingType) throws IOException {
        StoreUtils.assertValidKey(key);
        int requestSize = getHeaderSize(VoldemortOpCode.PUT_OP_CODE, storeName, routingType)
                          + VoldemortNativeProtocol.getKeyRequestSize(key)
                          + getVersionedSize(versioned);

        outputStream.writeInt(requestSize);
        super.writePutRequest(outputStream, storeName, key, versioned, routingType);
    }

    @Override
    protected int getVersionSize(Version version) {
        return 4 + VectorClockProtoSerializer.sizeInBytes(version);
    }

    @Override
    protected void writeVersion(DataOutputStream outputStream, Version version) throws IOException {
        byte[] bytes = VectorClockProtoSerializer.toBytes(version);
        outputStream.writeInt(bytes.length);
        outputStream.write(bytes);
    }

    @Override
    protected int getVersionedSize(Versioned<byte[]> versioned) {
        return 4 + VectorClockProtoSerializer.sizeInBytes(versioned);
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
