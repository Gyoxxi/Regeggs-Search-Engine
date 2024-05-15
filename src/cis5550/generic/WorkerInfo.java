package cis5550.generic;

public class WorkerInfo {
    private final String ip;
    private final int port;
    private long lastPingTime;

    public WorkerInfo(String ip, int port) {
        this.ip = ip;
        this.port = port;
        this.lastPingTime = System.currentTimeMillis();
    }

    public String getIp() {
        return ip;
    }

    public int getPort() {
        return port;
    }

    public long getLastPingTime() {
        return lastPingTime;
    }

    public void updateLastPingTime() {
        this.lastPingTime = System.currentTimeMillis();
    }

    @Override
    public String toString() {
        return ip + ":" + port;
    }
}
