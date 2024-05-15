package cis5550.kvs;

import static cis5550.webserver.Server.*;
public class Coordinator extends cis5550.generic.Coordinator {

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: java KVSCoordinator <port>");
            System.exit(1);
        }

        int portNum = Integer.parseInt(args[0]);
        port(portNum);

        registerRoutes();
        startCleanupTask();

        get("/", (req, res) -> {
            res.type("text/html");
            return "<html><head><title>KVS Coordinator</title></head>"
                    + "<body><h1>KVS Coordinator</h1>"
                    + workerTable()
                    + "</body></html>";
        });
    }

}

