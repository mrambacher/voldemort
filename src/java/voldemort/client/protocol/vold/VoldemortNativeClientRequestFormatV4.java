package voldemort.client.protocol.vold;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import voldemort.client.protocol.RequestFormatType;
import voldemort.protocol.vold.VoldemortNativeProtocol;
import voldemort.server.RequestRoutingType;
import voldemort.utils.ByteArray;
import voldemort.versioning.Versioned;

public class VoldemortNativeClientRequestFormatV4 extends VoldemortNativeClientRequestFormatV3 {

    @Override
    protected RequestFormatType getProtocol() {
        return RequestFormatType.VOLDEMORT_V4;
    }

    private void writeTransforms(DataOutputStream outputStream, byte[] transforms)
            throws IOException {
        if(transforms != null) {
            outputStream.writeBoolean(true);
            outputStream.writeInt(transforms.length);
            outputStream.write(transforms);
        } else {
            outputStream.writeBoolean(false);
        }
    }

    protected int getTransformSize(byte[] transforms) {
        return 1 + VoldemortNativeProtocol.getTransformSize(transforms); // Size
        // of
        // boolean
    }

    protected int getTransformSize(Map<ByteArray, byte[]> transforms) {
        int size = 1; // Size of boolean
        if(transforms != null) {
            size += 4; // sizeof int
            for(Map.Entry<ByteArray, byte[]> entry: transforms.entrySet()) {
                ByteArray key = entry.getKey();
                byte[] transform = entry.getValue();
                size += VoldemortNativeProtocol.getKeyRequestSize(key)
                        + VoldemortNativeProtocol.getTransformSize(transform);
            }
        }
        return size;
    }

    @Override
    protected int getRequestSize(String storeName,
                                 ByteArray key,
                                 byte[] transforms,
                                 RequestRoutingType routingType) throws IOException {
        int requestSize = super.getRequestSize(storeName, key, transforms, routingType);
        return requestSize + this.getTransformSize(transforms);
    }

    @Override
    public void writeGetRequest(DataOutputStream outputStream,
                                String storeName,
                                ByteArray key,
                                byte[] transforms,
                                RequestRoutingType routingType) throws IOException {
        super.writeGetRequest(outputStream, storeName, key, transforms, routingType);
        writeTransforms(outputStream, transforms);
    }

    @Override
    protected int putRequestSize(String storeName,
                                 ByteArray key,
                                 Versioned<byte[]> versioned,
                                 byte[] transforms,
                                 RequestRoutingType routingType) throws IOException {
        int requestSize = super.putRequestSize(storeName, key, versioned, transforms, routingType);
        return requestSize + this.getTransformSize(transforms);
    }

    @Override
    public void writePutRequest(DataOutputStream outputStream,
                                String storeName,
                                ByteArray key,
                                Versioned<byte[]> value,
                                byte[] transforms,
                                RequestRoutingType routingType) throws IOException {
        super.writePutRequest(outputStream, storeName, key, value, transforms, routingType);
        writeTransforms(outputStream, transforms);
    }

    @Override
    protected int getAllRequestSize(String storeName,
                                    Iterable<ByteArray> keys,
                                    Map<ByteArray, byte[]> transforms,
                                    RequestRoutingType routingType) throws IOException {
        int requestSize = super.getAllRequestSize(storeName, keys, transforms, routingType);
        return requestSize + this.getTransformSize(transforms);
    }

    @Override
    protected void writeGetAllRequest(DataOutputStream output,
                                      String storeName,
                                      List<ByteArray> keys,
                                      Map<ByteArray, byte[]> transforms,
                                      RequestRoutingType routingType) throws IOException {
        super.writeGetAllRequest(output, storeName, keys, transforms, routingType);
        if(transforms != null) {
            output.writeBoolean(true);
            output.writeInt(transforms.size());
            for(Map.Entry<ByteArray, byte[]> transform: transforms.entrySet()) {
                output.writeInt(transform.getKey().length());
                output.write(transform.getKey().get());
                if(transform.getValue() != null) {
                    output.writeInt(transform.getValue().length);
                    output.write(transform.getValue());
                } else
                    output.writeInt(0);
            }
        } else {
            output.writeBoolean(false);
        }
    }
}
