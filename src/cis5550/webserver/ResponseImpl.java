package cis5550.webserver;

import java.io.BufferedWriter;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class ResponseImpl implements Response {
    private int statusCode = 200;
    private String reasonPhrase = "OK";
    private boolean writeCalled = false;
    private String contentType = "text/plain";
    private byte[] body;
    private Map<String, ArrayList<String>> headerMap = new HashMap<>();
    private OutputStream outputStream;

    public ResponseImpl(OutputStream outputStream) {
        this.outputStream = outputStream;
    }

    public boolean isWriteCalled() {
        return writeCalled;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public String getReasonPhrase() {
        return reasonPhrase;
    }

    public String getContentType() {
        return contentType;
    }

    public byte[] getBody() {
        return body;
    }

    public Map<String, ArrayList<String>> getHeaderMap() {
        return headerMap;
    }

    @Override
    public void body(String body) {
        if (!writeCalled) {
            this.body = body.getBytes();
        }
    }

    @Override
    public void bodyAsBytes(byte[] bodyArg) {
        if (!writeCalled) {
            this.body = bodyArg;
        }
    }

    @Override
    public void header(String name, String value) {
        if (!writeCalled) {
            if (!headerMap.containsKey(name)) {
                headerMap.put(name, new ArrayList<>());
            }
            headerMap.get(name).add(value);
        }
    }

    @Override
    public void type(String contentType) {
        if (!writeCalled) {
            this.contentType = contentType;
            header("Content-Type", this.contentType);
        }
    }

    @Override
    public void status(int statusCode, String reasonPhrase) {
        if (!writeCalled) {
            this.statusCode = statusCode;
            this.reasonPhrase = reasonPhrase;
        }
    }

    @Override
    public void write(byte[] b) throws Exception {
        this.commit();
        outputStream.write(b);
        outputStream.flush();
    }

    @Override
    public void redirect(String url, int responseCode) {
        if (responseCode == 200) {
            this.status(responseCode, "OK");
        } else {
            this.status(responseCode, "Redirect");
        }

        this.header("location", url);

        try {
            this.commit();
        } catch (Exception var4) {
        }
    }

    void commit() throws Exception {
        if (!writeCalled) {
            PrintWriter writer = new PrintWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8), false);
            writer.print("HTTP/1.1 " + this.statusCode + " " + this.reasonPhrase + "\r\n");
            header("Connection", "close");
            for (Map.Entry<String, ArrayList<String>> headers : this.headerMap.entrySet()) {
                String headerKey = headers.getKey();
                for (String headerVal : headers.getValue()) {
                    writer.print(headerKey + ": " + headerVal + "\r\n");
                }
            }
            writer.print("\r\n");
            writer.flush();
            writeCalled = true;
        }
    }


    @Override
    public void halt(int statusCode, String reasonPhrase) {

    }



}
