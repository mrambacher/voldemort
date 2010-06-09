/*
 * Copyright 2010 Nokia Corporation. All rights reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package voldemort.server.niosocket;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

public class NioWorkerThread implements Runnable {

    private final BlockingQueue<AsyncRequestHandler> requests;
    private final AtomicBoolean isClosed;
    private int count;
    private final Logger logger = Logger.getLogger(getClass());

    public NioWorkerThread(BlockingQueue<AsyncRequestHandler> requests) {
        this.isClosed = new AtomicBoolean(false);
        this.requests = requests;
    }

    public void run() {
        try {
            while(true) {
                AsyncRequestHandler handler = requests.take();
                if(isClosed.get()) {
                    if(logger.isInfoEnabled())
                        logger.info("Closing NIO worker thread");
                    break;
                } else if(handler != null) {
                    count++;
                    handler.run();
                }
            }
        } catch(InterruptedException e) {

        }
    }

    public void close() {
        // Attempt to close, but if already closed, then we've been beaten to
        // the punch...
        if(!isClosed.compareAndSet(false, true))
            return;
    }
}
