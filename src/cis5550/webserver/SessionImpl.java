package cis5550.webserver;

import java.util.HashMap;
import java.util.Map;

public class SessionImpl implements Session{
    private String id;
    private long creationTime;
    private long lastAccessedTime;
    private int maxActiveInterval;
    private Map<String, Object> attributes;
    private boolean valid = true;

    public SessionImpl(String sessionId) {
        this.id = sessionId;
        this.creationTime = System.currentTimeMillis();
        this.lastAccessedTime = creationTime;
        this.attributes = new HashMap<>();
        this.maxActiveInterval = 300;
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public long creationTime() {
        return creationTime;
    }

    @Override
    public long lastAccessedTime() {
        return lastAccessedTime;
    }

    @Override
    public void maxActiveInterval(int seconds) {
        maxActiveInterval = seconds;
    }

    @Override
    public void invalidate() {
        this.valid = false;
    }

    @Override
    public Object attribute(String name) {
        return attributes.getOrDefault(name, null);
    }

    @Override
    public void attribute(String name, Object value) {
        attributes.putIfAbsent(name, value);
    }

    public void updateLastAccessedTime() {
        lastAccessedTime = System.currentTimeMillis();
    }

    public boolean isExpired() {
        if (!valid) {
            return true;
        }
        long currentTime = System.currentTimeMillis();
        return (currentTime - lastAccessedTime) > (maxActiveInterval * 1000L);
    }
}
