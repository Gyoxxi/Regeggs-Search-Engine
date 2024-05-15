package cis5550.webserver.http;

import cis5550.threading.ThreadPool;
import cis5550.tools.Logger;

import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;

public class HttpWorker extends Thread {

    private static final Logger logger = Logger.getLogger(HttpWorker.class);
    private BlockingQueue<HttpRequest> requestQueue;
    private boolean running;

    public HttpWorker(BlockingQueue<HttpRequest> requestQueue) {
        this.requestQueue = requestQueue;
        this.running = true;
    }

    public void run() {
        while (running) {
            try {
                HttpRequest httpRequest = requestQueue.take();
                logger.info("HttpWorker: A worker thread has been dequeued.");
                Socket clientSocket = httpRequest.getClientSocket();
                while (!clientSocket.isOutputShutdown()) {
                    httpRequest.parseAndProcessRequest(clientSocket.getInputStream());
                    if (!httpRequest.isKeepAlive()) {
                        break;
                    }
                }
                clientSocket.close();
                if (ThreadPool.isShutDown() && requestQueue.isEmpty()) {
                    break;
                }
            } catch (InterruptedException | IOException e) {
//                logger.error("HttpWorker: error occurred");
//                e.printStackTrace();
                Thread.currentThread().interrupt();
                if (ThreadPool.isShutDown()) {
                    break;
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void shutDown() {
        running = false;
        this.interrupt();
    }


}
