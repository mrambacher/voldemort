package voldemort.client.protocol.vold;

import java.io.DataOutputStream;
import java.io.IOException;

import voldemort.client.protocol.RequestFormatType;
import voldemort.server.RequestRoutingType;

public class VoldemortNativeClientRequestFormatV2 extends VoldemortNativeClientRequestFormat {

    @Override
    protected RequestFormatType getProtocol() {
        return RequestFormatType.VOLDEMORT_V2;
    }

    @Override
    protected void writeMessageHeader(DataOutputStream outputStream,
                                      byte operation,
                                      String storeName,
                                      RequestRoutingType routingType) throws IOException {
        super.writeMessageHeader(outputStream, operation, storeName, routingType);
    }
}
