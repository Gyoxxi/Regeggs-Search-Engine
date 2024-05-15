package cis5550.generic;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static cis5550.webserver.Server.get;


public abstract class Coordinator {
    private static final ConcurrentHashMap<String, WorkerInfo> workers = new ConcurrentHashMap<>();
    private static final long INACTIVITY_LIMIT = 15000;

    public static void startCleanupTask() {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        executor.scheduleAtFixedRate(() -> {
            long now = System.currentTimeMillis();
            workers.entrySet().removeIf(entry -> (now - entry.getValue().getLastPingTime()) > INACTIVITY_LIMIT);
        }, 0, 5, TimeUnit.SECONDS); // Check every 5 seconds
    }

    public static List<String> getWorkers() {
        long cutoff = System.currentTimeMillis() - INACTIVITY_LIMIT;
        return workers.values().stream()
                .filter(workerInfo -> workerInfo.getLastPingTime() >= cutoff)
                .map(WorkerInfo::toString)
                .collect(Collectors.toList());
    }

    public static String workerTable() {
        StringBuilder sb = new StringBuilder();
        sb.append("<table border=\"1\"><tr><th>Worker ID</th><th>IP</th><th>Port</th><th>Link</th></tr>");
        workers.forEach((id, workerInfo) -> {
            String ip = workerInfo.getIp();
            int port = workerInfo.getPort();

            sb.append("<tr>")
                    .append("<td>").append(id).append("</td>")
                    .append("<td>").append(ip).append("</td>")
                    .append("<td>").append(port).append("</td>")
                    .append("<td><a href='http://").append(ip).append(":").append(port).append("/'>Link</a></td>")
                    .append("</tr>");
        });
        sb.append("</table>");
        return sb.toString();
    }


    public static void registerRoutes() {
        // Route for /ping
        get("/ping", (req, res) -> {
            String workerId = req.queryParams("id");
            String portString = req.queryParams("port");
            String ip = req.ip();
            int port = -1;

            if (portString != null) {
                port = Integer.parseInt(portString);
            }

            if (workerId == null || portString == null) {
                res.status(400, "400 Bad Request");
                return "Bad Request";
            }

            WorkerInfo workerInfo = workers.getOrDefault(workerId, new WorkerInfo(ip, port));
            workerInfo.updateLastPingTime();
            workers.put(workerId, workerInfo);


            res.status(200, "200 OK");
            return "OK";
        });

        // Route for /workers
        get("/workers", (req, res) -> {
            StringBuilder sb = new StringBuilder();
            sb.append(workers.size()).append("\n");
            workers.forEach((id, workerInfo) -> {
                sb.append(id).append(",").append(workerInfo.toString()).append("\n");
            });
            return sb;
        });
    }

    public static String clientTable() {
        if (workers.isEmpty()) {
            return "<p>No active workers</p>";
        }

        StringBuilder doc = new StringBuilder();
        doc.append("<table border=\"1\">");
        doc.append("<tr><th>ID</th><th>Address</th><th>Last checkin</th></tr>");

        Set<String> sortedWorkers = new TreeSet<>(workers.keySet());
        for (String id : sortedWorkers) {
            WorkerInfo worker = workers.get(id);
            long lastPingTime = (System.currentTimeMillis() - workers.get(id).getLastPingTime()) / 1000;
            doc.append("<tr>");
            doc.append("<td>").append(id).append("</td>");
            doc.append("<td><a href=\"http://").append(worker).append("/\">").append(worker).append("</a></td>");
            doc.append("<td>").append(lastPingTime).append("seconds ago</td>");
            doc.append("</tr>");
        }

        doc.append("</table>");

        return doc.toString();
    }
}

