package cis5550.router;

import cis5550.webserver.Route;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class RouteManager {
    private static Map<String, Set<RouteInfo>> defaultRoutesTable = new HashMap<>();
    private static Map<String, Set<RouteInfo>> currRoutesTable = defaultRoutesTable;
    private static Map<String, Map<String, Set<RouteInfo>>> virtualHostRoutes = new HashMap<>();
    private Map<String, String> staticFileLocations = new HashMap<>();
    private String defaultStaticFileLocation;

    public void host(String virtualHost) {
        virtualHostRoutes.putIfAbsent(virtualHost, new HashMap<>());
        currRoutesTable = virtualHostRoutes.get(virtualHost);
    }

    public void registerRoute(String method, String path, Route route) {
        if (!currRoutesTable.containsKey(method)) {
            currRoutesTable.put(method, new HashSet<>());
        }
        currRoutesTable.get(method).add(new RouteInfo(path, route));
    }

    public Set<RouteInfo> getRoutesForRequest(String host, String method) {
        String hostName = host.split(":")[0];
        Map<String, Set<RouteInfo>> hostRoutes = virtualHostRoutes.getOrDefault(hostName, defaultRoutesTable);
        return hostRoutes.getOrDefault(method, new HashSet<>());
    }

    public void setStaticFilesLocation(String hostName, String location) {
        staticFileLocations.put(hostName, location);
    }

    public void setDefaultStaticFileLocation(String location) {
        this.defaultStaticFileLocation = location;
    }

    public String getStaticFileLocation(String host) {
        String hostWithoutPort = host.split(":")[0];
        return staticFileLocations.getOrDefault(hostWithoutPort, defaultStaticFileLocation);
    }

    public Map<String, Map<String, Set<RouteInfo>>> getVirtualHostRoutes() {
        return virtualHostRoutes;
    }
}
