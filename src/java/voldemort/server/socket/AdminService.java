package voldemort.server.socket;

import java.util.concurrent.ThreadPoolExecutor;

import voldemort.server.protocol.RequestHandlerFactory;

@Deprecated
public class AdminService extends SocketService {

    public AdminService(RequestHandlerFactory requestHandlerFactory,
                        ThreadPoolExecutor threadPool,
                        int port,
                        int socketBufferSize,
                        int socketListenQueueLength,
                        String serviceName,
                        boolean enableJmx) {
        super(requestHandlerFactory,
              threadPool,
              port,
              socketBufferSize,
              socketListenQueueLength,
              serviceName,
              enableJmx);
    }

}
