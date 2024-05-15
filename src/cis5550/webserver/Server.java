package cis5550.webserver;

import cis5550.router.RouteInfo;
import cis5550.router.RouteManager;
import cis5550.threading.ThreadPool;
import cis5550.tools.Logger;
import cis5550.webserver.http.HttpRequest;

import javax.net.ServerSocketFactory;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Server {

    public static class staticFiles{
        public static void location(String s) {
            if (server == null) {
                initializeServer();
            }
            if (virtualHost != null) {
                routeManager.setStaticFilesLocation(virtualHost, s);
            } else {
                routeManager.setDefaultStaticFileLocation(s);
            }
        }
    }

    private static final Logger logger = Logger.getLogger(Server.class);
    private static final int NUM_WORKERS = 100;
    private static Server server = null;
    private static boolean serverRunning = false;
    private static int serverPort = 80;
    private static int securePort = -1;
    private static RouteManager routeManager = new RouteManager();
    private static String virtualHost = null;
    private static Map<String, SessionImpl> sessionMap = new HashMap<>();

    public Server() {
    }

    public void run() throws Exception {
        try {
            if (securePort != -1) {
                SSLContext sslContext = createSSLContext();
                ServerSocketFactory factory = sslContext.getServerSocketFactory();
                ServerSocket serverSocketTLS = factory.createServerSocket(securePort);
                new Thread(() -> serverLoop(serverSocketTLS)).start();
            }
            ServerSocket serverSocket = new ServerSocket(serverPort);
            new Thread(() -> serverLoop(serverSocket)).start();
            Thread sessionThread = new Thread(() -> {
                while (!sessionMap.isEmpty()) {
                    try {
                        Thread.sleep(5 * 1000); // Sleep for 5 seconds
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    Iterator<Map.Entry<String, SessionImpl>> iter = sessionMap.entrySet().iterator();
                    while (iter.hasNext()) {
                        Map.Entry<String, SessionImpl> entry = iter.next();
                        SessionImpl session = entry.getValue();
                        if (session.isExpired()) {
                            iter.remove();
                        }
                    }
                }
            });
            sessionThread.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void serverLoop(ServerSocket serverSocket) {
        try {
            ThreadPool threadPool = new ThreadPool(30, NUM_WORKERS);
            while (serverRunning) {
                Socket clientSocket = serverSocket.accept();
                HttpRequest httpRequest = new HttpRequest(clientSocket, routeManager, sessionMap, this);
                threadPool.addRequest(httpRequest);
            }
        } catch (IOException e) {
            logger.error("Error in starting the server: " + e.getMessage());
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void initializeServer() {
        if (server == null) {
            server = new Server();
        }
        if (!serverRunning) {
            serverRunning = true;
            new Thread(() -> {
                try {
                    server.run();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }).start();
        }
    }

    public static void host(String hostName, String keyStoreFile, String password) {
        routeManager.host(hostName);
        virtualHost = hostName;
    }

    private static SSLContext createSSLContext() throws Exception {
        String pwd = "secret";
        KeyStore keyStore = KeyStore.getInstance("JKS");
        keyStore.load(new FileInputStream("keystore.jks"), pwd.toCharArray());
        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance("SunX509");
        keyManagerFactory.init(keyStore, pwd.toCharArray());
        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(keyManagerFactory.getKeyManagers(), null, null);
        return sslContext;
    }

    public static void get(String pathPattern, Route route) {
        initializeServer();
        routeManager.registerRoute("GET", pathPattern, route);
    }

    public static void post(String pathPattern, Route route) {
        initializeServer();
        routeManager.registerRoute("POST", pathPattern, route);
    }

    public static void put(String pathPattern, Route route) {
        initializeServer();
        routeManager.registerRoute("PUT", pathPattern, route);
    }

    public static void port(int portNum) {
        serverPort = portNum;
    }

    public static void securePort(int portNum) {
        securePort = portNum;
    }

    public void shutDownServer(Socket serverSocket) {
        serverRunning = false;
        ThreadPool.shutDown();
        if (serverSocket != null && !serverSocket.isClosed()) {
            try {
                serverSocket.close();
            } catch (IOException e) {
                logger.error("Server: error closing the server socket: " + e.getMessage());
            }
        }
        logger.info("Server has been shut down.");
    }

    public int getSecurePort() {
        return securePort;
    }
}