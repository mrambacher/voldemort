package voldemort.client.protocol.vold;

import java.io.DataOutputStream;
import java.io.IOException;

import voldemort.client.protocol.RequestFormatType;
import voldemort.serialization.VoldemortOpCode;
import voldemort.server.RequestRoutingType;

public class VoldemortNativeClientRequestFormatV2 extends VoldemortNativeClientRequestFormat {

    @Override
    protected RequestFormatType getProtocol() {
        return RequestFormatType.VOLDEMORT_V2;
    }

    @Override
    protected void writeMessageHeader(DataOutputStream outputStream,
                                      VoldemortOpCode operation,
                                      String storeName,
                                      RequestRoutingType routingType) throws IOException {
        outputStream.writeByte(operation.asCode());
        outputStream.writeUTF(storeName);
        outputStream.writeByte(routingType.getRoutingTypeCode());
    }
}
