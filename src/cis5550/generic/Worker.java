package cis5550.generic;

import cis5550.tools.HTTP;

import java.io.IOException;

public abstract class Worker {

    private static final long PING_FREQ_IN_MS = 5_000;

    public static void startPingThread(int port, String workerId, String coordinatorUrl) {
        Thread thread = new Thread(() -> {
            while (true) {
                try {
                    String url = "http://" + coordinatorUrl + "/ping?id=" + workerId + "&port=" + port;
                    HTTP.doRequest("GET", url, null);
                } catch (IOException e) {
                    System.err.println("Unable to ping coordinator at " + coordinatorUrl);
                }

                try {
                    Thread.sleep(PING_FREQ_IN_MS);
                } catch (InterruptedException ignored) {
                }
            }
        });

        thread.start();
    }
}
