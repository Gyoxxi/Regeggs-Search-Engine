package cis5550.threading;

import cis5550.tools.Logger;
import cis5550.webserver.http.HttpRequest;
import cis5550.webserver.http.HttpWorker;

import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;

public class ThreadPool {

    private static final Logger logger = Logger.getLogger(ThreadPool.class);
    private static CopyOnWriteArrayList<HttpWorker> threadPool = new CopyOnWriteArrayList<>();
    private BlockingQueue<HttpRequest> requestQueue;
    private static boolean shutDown = false;
    private static boolean workerStarted = false;

    public ThreadPool(int capacity, int numThreads) {
        this.requestQueue = new LinkedBlockingQueue<>(capacity);
        if (!workerStarted) {
            for (int i = 0; i < numThreads; i++) {
                HttpWorker workerThread = new HttpWorker(requestQueue);
                threadPool.add(workerThread);
                workerThread.start();
            }
            workerStarted = true;
        }
    }

    public static void shutDown() {
        shutDown = true;
        for (HttpWorker worker : threadPool) {
            worker.shutDown();
            worker.interrupt();
        }
        logger.info("Shut down workers in thread pool.");
    }

    public void addRequest(HttpRequest request) {
        try {
            logger.info("ThreadPool: Adding request to the queue");
            requestQueue.put(request);
        } catch (InterruptedException e) {
            logger.error("ThreadPool: Failed to add request to the queue: ", e);
            Thread.currentThread().interrupt();
        }
    }

    public static boolean isShutDown() {
        return shutDown;
    }

    public static void setIsShutDown(boolean shutDown) {
        ThreadPool.shutDown = shutDown;
    }
}