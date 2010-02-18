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
    protected RequestRoutingType getRoutingType(DataInputStream inputStream) throws IOException {
        return super.getRoutingType(inputStream);
    }
}
