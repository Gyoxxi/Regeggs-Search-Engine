package cis5550.router;

import cis5550.webserver.Route;

import java.util.Objects;

public class RouteInfo {
    private final String pathPattern;
    private final Route route;

    public RouteInfo(String pathPattern, Route route) {
        this.pathPattern = pathPattern;
        this.route = route;
    }

    public String getPathPattern() {
        return pathPattern;
    }

    public Route getRoute() {
        return route;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RouteInfo routeInfo = (RouteInfo) o;
        return pathPattern.equals(routeInfo.pathPattern) && route.equals(routeInfo.route);
    }

    @Override
    public int hashCode() {
        return Objects.hash(pathPattern, route);
    }
}
