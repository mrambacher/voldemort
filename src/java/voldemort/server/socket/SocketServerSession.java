package voldemort.server.socket;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Map;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.protocol.RequestFormatType;
import voldemort.server.protocol.RequestHandler;
import voldemort.server.protocol.RequestHandlerFactory;
import voldemort.server.protocol.StreamRequestHandler;
import voldemort.server.protocol.StreamRequestHandler.StreamRequestHandlerState;
import voldemort.utils.ByteUtils;

/**
 * Represents a session of interaction between the server and the client. This
 * begins with protocol negotiation and then a seriest of client requests
 * followed by server responses. The negotiation is handled by the session
 * object, which will choose an appropriate request handler to handle the actual
 * request/response.
 * 
 * 
 */
public class SocketServerSession implements Runnable {

    private final Logger logger = Logger.getLogger(SocketServerSession.class);

    private final Map<Long, SocketServerSession> activeSessions;
    private final long sessionId;
    private final Socket socket;
    private final RequestHandlerFactory handlerFactory;
    private volatile boolean isClosed = false;

    public SocketServerSession(Map<Long, SocketServerSession> activeSessions,
                               Socket socket,
                               RequestHandlerFactory handlerFactory,
                               long id) {
        this.activeSessions = activeSessions;
        this.socket = socket;
        this.handlerFactory = handlerFactory;
        this.sessionId = id;
    }

    public Socket getSocket() {
        return socket;
    }

    private boolean isInterrupted() {
        return Thread.currentThread().isInterrupted();
    }

    public void run() {
        DataInputStream inputStream = null;
        DataOutputStream outputStream = null;
        RequestHandler handler = null;
        try {
            activeSessions.put(sessionId, this);
            inputStream = new DataInputStream(new BufferedInputStream(socket.getInputStream(),
                                                                      64000));
            outputStream = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream(),
                                                                         64000));

            RequestFormatType protocol = negotiateProtocol(inputStream, outputStream);
            if(protocol != null) {
                handler = handlerFactory.getRequestHandler(protocol);
                logger.info("Client " + socket.getRemoteSocketAddress()
                            + " connected successfully with protocol " + protocol.getCode());
            } else {
                logger.info("Client " + socket.getRemoteSocketAddress()
                            + " failed to negotiate protocol ");
                this.close();

            }
            while(!isInterrupted() && !socket.isClosed() && !isClosed) {
                StreamRequestHandler srh = handler.handleRequest(inputStream, outputStream);

                if(srh != null) {
                    if(logger.isTraceEnabled())
                        logger.trace("Request is streaming");

                    StreamRequestHandlerState srhs = null;

                    try {
                        do {
                            if(logger.isTraceEnabled())
                                logger.trace("About to enter streaming request handler");

                            srhs = srh.handleRequest(inputStream, outputStream);

                            if(logger.isTraceEnabled())
                                logger.trace("Finished invocation of streaming request handler, result is "
                                             + srhs);

                        } while(srhs != StreamRequestHandlerState.COMPLETE);
                    } catch(VoldemortException e) {
                        srh.handleError(outputStream, e);
                        outputStream.flush();

                        break;
                    } finally {
                        srh.close(outputStream);
                    }
                }

                outputStream.flush();
            }
            if(isInterrupted())
                logger.info(Thread.currentThread().getName()
                            + " has been interrupted, closing session.");
        } catch(EOFException e) {
            logger.info("Client " + socket.getRemoteSocketAddress() + " disconnected - "
                        + e.getMessage(), e);
        } catch(IOException e) {
            // if this is an unexpected
            if(!isClosed)
                logger.error(e);
        } finally {
            try {
                if(!socket.isClosed())
                    socket.close();
            } catch(Exception e) {
                logger.error("Error while closing socket - " + e.getMessage(), e);
            }
            if(null != inputStream) {
                try {
                    inputStream.close();
                } catch(IOException e) {
                    logger.error("Error while Data Input Stream - " + e.getMessage(), e);
                }
                inputStream = null;
            }
            if(null != outputStream) {
                try {
                    outputStream.close();
                } catch(IOException e) {
                    logger.error("Error while Data Output Stream - " + e.getMessage(), e);
                }
                outputStream = null;
            }
            // now remove ourselves from the set of active sessions
            this.activeSessions.remove(sessionId);
        }
    }

    private RequestFormatType negotiateProtocol(InputStream input, OutputStream output)
            throws IOException {
        RequestFormatType requestFormat = null;
        input.mark(3);
        byte[] bytes = new byte[3];
        ByteUtils.read(input, bytes);
        try {
            byte[] protoBytes = new byte[] { bytes[0], bytes[1] };
            byte[] versionBytes = { bytes[2] };

            String proto = ByteUtils.getString(protoBytes, "UTF-8");
            int version = Integer.parseInt(ByteUtils.getString(versionBytes, "UTF-8"));
            requestFormat = RequestFormatType.fromCode(proto, version);
            if(version == requestFormat.getVersion()) {
                output.write(ByteUtils.getBytes("ok", "UTF-8"));
            } else {
                output.write(ByteUtils.getBytes(requestFormat.getVersionAsString(), "UTF-8"));
            }
            output.flush();
        } catch(IllegalArgumentException e) {
            output.write(ByteUtils.getBytes("no", "UTF-8"));
            output.flush();
        }
        return requestFormat;
    }

    public void close() throws IOException {
        this.isClosed = true;
        this.socket.close();
    }
}
