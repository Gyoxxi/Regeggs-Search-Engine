package cis5550.webserver.http;

import cis5550.tools.Logger;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class HttpResponse {
    private static final Logger logger = Logger.getLogger(HttpRequest.class);
    private int status;
    private String statusMessage;
    private String contentType;
    private int fileLength;
    private byte[] content;
    private int responseContentLength;
    private String lastModified;
    private long startByte;
    private long endByte;
    private Map<String, String> headers = new HashMap<>();

    private void generateStatusMessage() {
        logger.info("Sending response with status code " + status);
        String message = switch (status) {
            case 200 -> "OK";
            case 206 -> "Partial Content";
            case 304 -> "Error 304 (Not Modified)";
            case 400 -> "Error 400 (Bad Request)";
            case 403 -> "Error 403 (Forbidden)";
            case 404 -> "Error 404 (Not Found)";
            case 405 -> "Error 405 (Method Not Allowed)";
            case 500 -> "Error 500 (Internal Server Error)";
            case 501 -> "Error 501 (Not Implemented)";
            case 505 -> "Error 505 (Version Not Supported)";
            default -> null;
        };
        setStatusMessage(message);
    }

    public void writeDynamicRequestHeaders(OutputStream outputStream) throws IOException {
        logger.info("Sending response with status code " + status);
        generateStatusMessage();
        StringBuilder headerBuilder = new StringBuilder("HTTP/1.1 " + status + " " + statusMessage + "\r\n");
        for (Map.Entry<String, String> header : headers.entrySet()) {
            headerBuilder.append(header.getKey()).append(": ").append(header.getValue()).append("\r\n");
        }
        headerBuilder.append("\r\n");

        byte[] responseBytes = headerBuilder.toString().getBytes(StandardCharsets.UTF_8);
        outputStream.write(responseBytes);
        outputStream.flush();
    }

    public void writeHeaders(OutputStream outputStream) throws IOException {
        logger.info("Sending response with status code " + status);
        generateStatusMessage();
        StringBuilder headerBuilder = new StringBuilder("HTTP/1.1 " + status + " " + statusMessage + "\r\n");

        if (status == 304) {
            headerBuilder.append("Server: ").append(headers.get("Server")).append("\r\n");
        } else {
            if (status == 206) {
                responseContentLength = (int)(endByte - startByte + 1);
                setHeaders("Content-Range", "bytes " + startByte + "-" + endByte + "/" + fileLength);
                setHeaders("Last-Modified", lastModified);
            }
            setHeaders("Content-Length", String.valueOf(responseContentLength));
            for (Map.Entry<String, String> header : headers.entrySet()) {
                headerBuilder.append(header.getKey()).append(": ").append(header.getValue()).append("\r\n");
            }
        }

        headerBuilder.append("\r\n");

        byte[] responseBytes = headerBuilder.toString().getBytes(StandardCharsets.UTF_8);
        outputStream.write(responseBytes);
        outputStream.flush();
    }

    public void writeBody(OutputStream outputStream, String method) throws IOException {
        if (method != null && method.equals("GET") && content != null && content.length > 0) {
            logger.info("HttpResponse: writing file content in response");
            outputStream.write(content);
            outputStream.flush();
        }
        logger.info("Response sent. Waiting for next request...");
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public String getStatusMessage() {
        return statusMessage;
    }

    public void setStatusMessage(String statusMessage) {
        this.statusMessage = statusMessage;
    }

    public String getContentType() {
        return contentType;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    public byte[] getContent() {
        return content;
    }

    public void setContent(byte[] content) {
        this.content = content;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public void setHeaders(String key, String value) {
        this.headers.put(key, value);
    }

    public long getStartByte() {
        return startByte;
    }

    public void setStartByte(long startByte) {
        this.startByte = startByte;
    }

    public long getEndByte() {
        return endByte;
    }

    public void setEndByte(long endByte) {
        this.endByte = endByte;
    }

    public int getFileLength() {
        return fileLength;
    }

    public void setFileLength(int fileLength) {
        this.fileLength = fileLength;
    }

    public String getLastModified() {
        return lastModified;
    }

    public void setLastModified(String lastModified) {
        this.lastModified = lastModified;
    }

    public int getResponseContentLength() {
        return responseContentLength;
    }

    public void setResponseContentLength(int responseContentLength) {
        this.responseContentLength = responseContentLength;
    }
}