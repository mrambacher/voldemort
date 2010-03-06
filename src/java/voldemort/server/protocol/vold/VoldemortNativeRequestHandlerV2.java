package voldemort.server.protocol.vold;

import java.io.DataInputStream;
import java.io.IOException;

import voldemort.server.RequestRoutingType;
import voldemort.server.StoreRepository;
import voldemort.store.ErrorCodeMapper;

public class VoldemortNativeRequestHandlerV2 extends VoldemortNativeRequestHandler {

    public VoldemortNativeRequestHandlerV2(ErrorCodeMapper errorMapper, StoreRepository repository) {
        super(errorMapper, repository);
    }

    @Override
    protected void checkCompleteRequestHeader(DataInputStream inputStream) throws IOException {
        // Read the store name in, but just to skip the bytes.
        inputStream.readUTF();

        // Read the routing type byte.
        inputStream.readByte();
    }

    @Override
    protected RequestRoutingType getRoutingType(DataInputStream inputStream) throws IOException {
        int routingTypeCode = inputStream.readByte();
        RequestRoutingType routingType = RequestRoutingType.getRequestRoutingType(routingTypeCode);
        return routingType;
    }
}
