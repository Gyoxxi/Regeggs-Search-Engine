package cis5550.webserver.http;

import cis5550.router.RouteInfo;
import cis5550.router.RouteManager;
import cis5550.tools.Logger;
import cis5550.webserver.*;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class HttpRequest {
    private Socket clientSocket;
    private String method;
    private String url;
    private String version;
    private Map<String, String> headersMap = new HashMap<>();
    private Map<String, SessionImpl> sessionMap;
    private boolean keepAlive = true;
    private Server server;
    private String virtualHost;
    private RouteManager routeManager;
    private Map<String, String> pathParameters;
    private String curSessionId;
    private static final Logger logger = Logger.getLogger(HttpRequest.class);
    private static final Set<String> SUPPORTED_METHODS = new HashSet<>(Arrays.asList("GET", "POST", "HEAD", "PUT"));
    private static final String CONTENT_LENGTH = "Content-Length";
    private static final String CONTENT_TYPE = "Content-Type";
    private static final String CONNECTION = "Connection";
    private static final String HOST = "Host";
    private static final String MODIFIED = "If-Modified-Since";
    private static final String RANGE = "Range";
    private static final String COOKIE = "Cookie";
    private static final Map<String, String> CONTENT_TYPE_MAP = new HashMap<>();

    static {
        CONTENT_TYPE_MAP.put("jpg", "image/jpg");
        CONTENT_TYPE_MAP.put("jpeg", "image/jpeg");
        CONTENT_TYPE_MAP.put("png", "image/png");
        CONTENT_TYPE_MAP.put("svg", "image/svg+xml");
        CONTENT_TYPE_MAP.put("ico", "image/x-icon");
        CONTENT_TYPE_MAP.put("css", "text/css");
        CONTENT_TYPE_MAP.put("js", "application/javascript");
        CONTENT_TYPE_MAP.put("txt", "text/plain");
        CONTENT_TYPE_MAP.put("html", "text/html");
    }

    public HttpRequest(Socket clientSocket, RouteManager routeManager, Map<String, SessionImpl> sessionMap, Server server) {
        this.clientSocket = clientSocket;
        this.routeManager = routeManager;
        this.sessionMap = sessionMap;
        this.server = server;
    }

    public void parseAndProcessRequest(InputStream inputStream) throws Exception {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        int lastChar = -1;
        int curChar;
        boolean foundCRLF = false;

        while ((curChar = inputStream.read()) != -1) {
            buffer.write(curChar);
            if ((lastChar == '\r' && curChar == '\n') && !foundCRLF) {
                foundCRLF = true;
            } else if (curChar == '\r' && lastChar != '\n') {
                foundCRLF = false;
            } else if (lastChar == '\r' && curChar == '\n' && foundCRLF) {
                break;
            }
            lastChar = curChar;
        }
        if (curChar == -1) {
            logger.warn("End of stream. Closing connection.");
            setKeepAlive(false);
            return;
        }

        ByteArrayInputStream headerInputStream = new ByteArrayInputStream(buffer.toByteArray());
        InputStreamReader headerInputStreamReader = new InputStreamReader(headerInputStream, StandardCharsets.UTF_8);
        BufferedReader headerBufferedReader = new BufferedReader(headerInputStreamReader);

        int errorStatus = hasErrors(headerBufferedReader);
        HttpResponse errorResponse = new HttpResponse();
        addResponseHeaders(errorResponse, "text/plain");
        if (errorStatus != 200) {
            errorResponse.setStatus(errorStatus);
            errorResponse.writeHeaders(clientSocket.getOutputStream());
            setKeepAlive(false);
            return;
        }

        String headerLine;
        int contentLengthVal = 0;
        boolean foundHostHeader = false;
        boolean queryParamInBody = false;
        while ((headerLine = headerBufferedReader.readLine()) != null && !headerLine.isEmpty()) {
            logger.info("Header line: " + headerLine);
            int colonIndex = headerLine.indexOf(":");
            if (colonIndex != -1) {
                String headerName = headerLine.substring(0, colonIndex).trim().toLowerCase();
                String headerVal = headerLine.substring(colonIndex + 1). trim();
                headersMap.put(headerName, headerVal);
                if (headerName.equalsIgnoreCase(CONTENT_LENGTH)) {
                    contentLengthVal = Integer.parseInt(headerVal);
                }
                if (headerName.equalsIgnoreCase(CONNECTION) && headerVal.equalsIgnoreCase("close")) {
                    setKeepAlive(false);
                    return;
                }
                if (headerName.equalsIgnoreCase(HOST)) {
                    foundHostHeader = true;
                    if (virtualHost == null) {
                        virtualHost = headerVal;
                    } else if (!virtualHost.equalsIgnoreCase(headerVal)) {
                        errorResponse.setStatus(404);
                        errorResponse.writeHeaders(clientSocket.getOutputStream());
                        setKeepAlive(false);
                        return;
                    }
                }
                if (headerName.equalsIgnoreCase(CONTENT_TYPE) && headerVal.equalsIgnoreCase("application/x-www-form-urlencoded")) {
                    queryParamInBody = true;
                }
            }
        }

        if (!foundHostHeader) {
            errorResponse.setStatus(400);
            errorResponse.writeHeaders(clientSocket.getOutputStream());
            setKeepAlive(false);
            return;
        }

        if (headersMap.containsKey(COOKIE.toLowerCase())) {
            String sessionId = extractSessionId(headersMap.get(COOKIE.toLowerCase()));
            if (sessionId != null && sessionMap.containsKey(sessionId)) {
                SessionImpl session = sessionMap.get(sessionId);
                if (!session.isExpired()) {
                    session.updateLastAccessedTime();
                    curSessionId = sessionId;
                }
            }
        }

        byte[] body = null;
        if (contentLengthVal > 0) {
            body = new byte[contentLengthVal];
            int totalRead = 0;
            while (totalRead < contentLengthVal) {
                int read = inputStream.read(body, totalRead, contentLengthVal - totalRead);
                if (read == -1) {
                    break;
                }
                totalRead += read;
            }
            logger.info("Read " + totalRead + " bytes from request body.");
        }

        Route route = findMatchingRoute();
        if (route != null) {
            handleDynamicRequest(route, body, queryParamInBody, errorResponse);
            return;
        }

        handleStaticFile(errorResponse);

    }

    private void handleDynamicRequest(Route route, byte[] body, boolean queryParamInBody, HttpResponse errorResponse) throws IOException {
        Map<String, String> queryParameters = findQueryParams(url);
        if (queryParamInBody) {
            queryParameters.putAll(findQueryParams(new String(body, StandardCharsets.UTF_8)));
        }
        RequestImpl request = new RequestImpl(method, url.split("\\?")[0], version, headersMap, queryParameters, pathParameters,
                new InetSocketAddress(clientSocket.getInetAddress(), clientSocket.getPort()), body, server, sessionMap);
        request.setSessionId(curSessionId);
        ResponseImpl response = new ResponseImpl(clientSocket.getOutputStream());
        try {
            Object result = route.handle(request, response);
            byte[] responseBody = null;
            if (!response.isWriteCalled()) {
                if (result != null) {
                    responseBody = result.toString().getBytes();
                } else if (response.getBody() != null) {
                    responseBody = response.getBody();
                }
                generateDynamicHeaders(request, response, responseBody, clientSocket.getOutputStream());
                if (responseBody != null) {
                    clientSocket.getOutputStream().write(responseBody);
                    clientSocket.getOutputStream().flush();
                }
            }
        } catch (Exception e) {
            errorResponse.setStatus(500);
            errorResponse.writeHeaders(clientSocket.getOutputStream());
            setKeepAlive(false);
            return;
        }

        for (Map.Entry<String, ArrayList<String>> entry : response.getHeaderMap().entrySet()) {
            String headerKey = entry.getKey();
            if (headerKey.equalsIgnoreCase(CONNECTION)) {
                for (String headerVal : entry.getValue()) {
                    if (headerVal.equalsIgnoreCase("close")) {
                        setKeepAlive(false);
                        return;
                    }
                }
            }
        }
    }

    private void handleStaticFile(HttpResponse errorResponse) throws IOException {
        String directory = routeManager.getStaticFileLocation(virtualHost);
        String filePath = directory + url;
        logger.info("File path: " + filePath);
        if (filePath.contains("..")) {
            errorResponse.setStatus(403);
            errorResponse.writeHeaders(clientSocket.getOutputStream());
            setKeepAlive(false);
            return;
        }
        File file = new File(filePath);
        logger.info("file: " + file.getAbsolutePath());
        if (!file.exists()) {
            logger.info("Here: file no found");
            errorResponse.setStatus(404);
            errorResponse.writeHeaders(clientSocket.getOutputStream());
            setKeepAlive(false);
            return;
        }
        if (!Files.isReadable(Path.of(filePath))) {
            errorResponse.setStatus(403);
            errorResponse.writeHeaders(clientSocket.getOutputStream());
            setKeepAlive(false);
            return;
        }

        logger.info("HttpRequest: accessing file with the path " + directory + url);

        if (headersMap.containsKey(MODIFIED.toLowerCase())) {
            String modifiedSinceHeader = headersMap.get(MODIFIED.toLowerCase());
            int errorStatus = handleModifiedSince(modifiedSinceHeader, file);
            if (errorStatus != 200) {
                errorResponse.setStatus(errorStatus);
                errorResponse.writeHeaders(clientSocket.getOutputStream());
                setKeepAlive(false);
                return;
            }
        }

        long[] range = null;
        if (headersMap.containsKey(RANGE.toLowerCase())) {
            String rangeHeader = headersMap.get(RANGE.toLowerCase());
            if (rangeHeader != null) {
                range = handleRange(rangeHeader, file.length());
            }

        }

        byte[] fileContent = Files.readAllBytes(file.toPath());

        String contentType = determineContentType(url);
        HttpResponse fileResponse = new HttpResponse();
        fileResponse.setStatus((range != null) ? 206 : 200);
        addResponseHeaders(fileResponse, contentType);
        if (range != null) {
            fileResponse.setStartByte(range[0]);
            fileResponse.setEndByte(range[1]);
            fileResponse.setFileLength((int)file.length());
            if (file.lastModified() > 0) {
                SimpleDateFormat dateFormat = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz", Locale.US);
                dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
                fileResponse.setLastModified(dateFormat.format(new Date(file.lastModified())));
            }
            fileResponse.setContent(Arrays.copyOfRange(fileContent, (int)range[0], (int)range[1] + 1));
        } else {
            fileResponse.setResponseContentLength(fileContent.length);
            fileResponse.setContent(fileContent);
        }
        fileResponse.writeHeaders(clientSocket.getOutputStream());
        fileResponse.writeBody(clientSocket.getOutputStream(), method);
    }

    private int hasErrors(BufferedReader headerBufferedReader) throws IOException {

        String requestLine = headerBufferedReader.readLine();
        logger.info("Request line: " + requestLine);
        if (requestLine == null || requestLine.isEmpty()) {
            logger.warn("Received null request line.");
            return 400;
        }

        String[] requestParts = requestLine.split(" ");
        if (requestParts.length != 3) {
            return 400;
        }
        String method = requestParts[0];
        String fileName = requestParts[1];
        String httpVersion = requestParts[2];
        this.method = method;
        this.url = fileName;
        this.version = httpVersion;

        if (!SUPPORTED_METHODS.contains(method)) {
            return 501;
        }

        if (!httpVersion.equals("HTTP/1.1")) {
            return 505;
        }
        return 200;
    }

    private void generateDynamicHeaders(RequestImpl request, ResponseImpl response, byte[] responseBody, OutputStream outputStream) throws IOException {
        HttpResponse httpResponse = new HttpResponse();
        httpResponse.setStatus(response.getStatusCode());
        httpResponse.setStatusMessage(response.getReasonPhrase());
        httpResponse.setHeaders("Content-Type", response.getContentType());
        int responseContentLength = ((responseBody != null) ? responseBody.length : 0);
        httpResponse.setHeaders("Content-Length", String.valueOf(responseContentLength));
        if (request.isCreatedNewSession()) {
            String cookieHeader = String.format("SessionID=%s; HttpOnly; SameSite=Strict", request.getSessionId());
            if (server.getSecurePort() != -1) {
                cookieHeader += "; Secure";
            }
            httpResponse.setHeaders("Set-Cookie", cookieHeader);
        }
        if (method.equals("PUT")) {
            if (response.getHeaderMap() != null) {
                for (Map.Entry<String, ArrayList<String>> entry : response.getHeaderMap().entrySet()) {
                    String headerKey = entry.getKey();
                    for (String headerVal : entry.getValue()) {
                        httpResponse.setHeaders(headerKey, headerVal);
                    }
                }
            }
        }
        httpResponse.writeDynamicRequestHeaders(outputStream);
    }


    private String extractSessionId(String cookieHeader) {
        if (cookieHeader != null && !cookieHeader.isEmpty()) {
            String[] cookies = cookieHeader.split("; ");
            for (String cookie : cookies) {
                String[] parts = cookie.split("=");
                if (parts.length == 2 && "SessionID".equalsIgnoreCase(parts[0])) {
                    return parts[1];
                }
            }
        }
        return null;
    }

    private Route findMatchingRoute() throws IOException {
        Set<RouteInfo> routes = routeManager.getRoutesForRequest(virtualHost, method);
        for (RouteInfo routeInfo : routes) {
            String pathOnly = url.split("\\?")[0];
            if (pathOnly.equalsIgnoreCase(routeInfo.getPathPattern())) {
                return routeInfo.getRoute();
            } else {
                Map<String, String> pathParams = matchPathPattern(pathOnly, routeInfo.getPathPattern());
                if (pathParams != null) {
                    pathParameters = pathParams;
                    return routeInfo.getRoute();
                }
            }
        }
        return null;
    }

    private Map<String, String> matchPathPattern(String requestedPath, String pattern) {
        String normalizedRequestedPath = requestedPath.startsWith("/") ? requestedPath.substring(1) : requestedPath;
        String normalizedPattern = pattern.startsWith("/") ? pattern.substring(1) : pattern;
        String[] requestedSegments = normalizedRequestedPath.split("/");
        String[] patternSegments = normalizedPattern.split("/");
        if (requestedSegments.length != patternSegments.length) {
            return null;
        }
        Map<String, String> pathParams = new HashMap<>();
        for (int i = 0; i < requestedSegments.length; i++) {
            String requestedSegment = requestedSegments[i];
            String patternSegment = patternSegments[i];
            if (patternSegment.startsWith(":")) {
                String parameter = patternSegment.substring(1);
                pathParams.put(parameter, requestedSegment);
            } else if (!requestedSegment.equals(patternSegment)) {
                return null;
            }
        }
        return pathParams;
    }

    private Map<String, String> findQueryParams(String query) throws UnsupportedEncodingException {
        Map<String, String> queryParams = new HashMap<>();
        if (query == null || query.isEmpty()) {
            return queryParams;
        }
        int queryStart = query.indexOf("?");
        String queryOnly = queryStart >= 0 ? query.substring(queryStart + 1) : query;
        String[] pairs = queryOnly.split("&");
        for (String pair : pairs) {
            int idx = pair.indexOf("=");
            if (idx != -1) {
                String key = pair.substring(0, idx);
                String value = pair.substring(idx + 1);
                queryParams.put(URLDecoder.decode(key, "UTF-8"), URLDecoder.decode(value, "UTF-8"));
            }
        }
        return queryParams;
    }

    private void addResponseHeaders(HttpResponse response, String contentType) {
        response.setHeaders("Content-Type", contentType);
        response.setHeaders("Server", "localhost");
    }

    private String determineContentType(String fileName) {
        int dotIndex = fileName.lastIndexOf(".");
        if (dotIndex == -1) {
            return "application/octet-stream";
        }
        String extension = fileName.substring(dotIndex + 1);
        return CONTENT_TYPE_MAP.getOrDefault(extension, "application/octet-stream");
    }

    private int handleModifiedSince(String modifiedSinceHeader, File file) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz", Locale.US);
        dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        try {
            Date modifiedSinceDate = dateFormat.parse(modifiedSinceHeader);
            long fileLastModified = file.lastModified();
            if (fileLastModified <= modifiedSinceDate.getTime()) {
                logger.error("HttpRequest: The file is not modified since");
                return 304;
            }
            if (modifiedSinceDate.after(new Date())) {
                logger.error("HttpRequest: The last modified date is later than the server's current time.");
                return 400;
            }

        } catch (ParseException e) {
            logger.error("HttpRequest: Could not parse if modified since date: " + e.getMessage());
            return 400;
        }
        return 200;
    }

    private long[] handleRange(String rangeHeader, long fileLength) {
        String byteRange = rangeHeader.substring(6);
        int indexOfDash = byteRange.indexOf("-");
        long start = 0;
        long end = fileLength - 1;
        if (indexOfDash == 0) {
            long lastNBytes = Long.parseLong(byteRange.substring(indexOfDash + 1));
            start = fileLength - lastNBytes;
        } else if (indexOfDash == byteRange.length() - 1) {
            start = Long.parseLong(byteRange.substring(0, indexOfDash));
        } else {
            String[] numbers = byteRange.split("-");
            start = Long.parseLong(numbers[0]);
            end = Long.parseLong(numbers[1]);
        }
        return new long[]{start, end};
    }

    public boolean isKeepAlive() {
        return keepAlive;
    }

    public void setKeepAlive(boolean keepAlive) {
        this.keepAlive = keepAlive;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Map<String, String> getHeadersMap() {
        return headersMap;
    }

    public void setHeadersMap(Map<String, String> headersMap) {
        this.headersMap = headersMap;
    }

    public Socket getClientSocket() {
        return clientSocket;
    }

    public void setClientSocket(Socket clientSocket) {
        this.clientSocket = clientSocket;
    }
}
