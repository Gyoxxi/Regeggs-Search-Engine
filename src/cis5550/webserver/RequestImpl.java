package cis5550.webserver;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Map;
import java.util.Set;

// Provided as part of the framework code

public class RequestImpl implements Request {
  String method;
  String url;
  String protocol;
  InetSocketAddress remoteAddr;
  Map<String,String> headers;
  Map<String,String> queryParams;
  Map<String,String> params;
  byte bodyRaw[];
  Server server;
  Map<String, SessionImpl> sessionMap;
  String sessionId;
  String CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
  int SESSION_ID_LENGTH = 20;
  SecureRandom random = new SecureRandom();
  boolean createdNewSession = false;

  public RequestImpl(String methodArg, String urlArg, String protocolArg, Map<String,String> headersArg, Map<String,String> queryParamsArg, Map<String,String> paramsArg, InetSocketAddress remoteAddrArg,
                     byte bodyRawArg[], Server serverArg, Map<String, SessionImpl> sessionMapArg) {
    method = methodArg;
    url = urlArg;
    remoteAddr = remoteAddrArg;
    protocol = protocolArg;
    headers = headersArg;
    queryParams = queryParamsArg;
    params = paramsArg;
    bodyRaw = bodyRawArg;
    server = serverArg;
    sessionMap = sessionMapArg;
  }

  public String requestMethod() {
  	return method;
  }
  public void setParams(Map<String,String> paramsArg) {
    params = paramsArg;
  }
  public int port() {
  	return remoteAddr.getPort();
  }
  public String url() {
  	return url;
  }
  public String protocol() {
  	return protocol;
  }
  public String contentType() {
  	return headers.get("content-type");
  }
  public String ip() {
  	return remoteAddr.getAddress().getHostAddress();
  }
  public String body() {
    return new String(bodyRaw, StandardCharsets.UTF_8);
  }
  public byte[] bodyAsBytes() {
  	return bodyRaw;
  }
  public int contentLength() {
  	return bodyRaw.length;
  }
  public String headers(String name) {
  	return headers.get(name.toLowerCase());
  }
  public Set<String> headers() {
  	return headers.keySet();
  }
  public String queryParams(String param) {
  	return queryParams.get(param);
  }
  public Set<String> queryParams() {
  	return queryParams.keySet();
  }
  public String params(String param) {
    return params.get(param);
  }

  @Override
  public Session session() {
    if (sessionId != null && sessionMap.containsKey(sessionId)) {
      return sessionMap.get(sessionId);
    } else {
      String newId = generateSessionId();
      sessionId = newId;
      SessionImpl session = new SessionImpl(newId);
      sessionMap.put(newId, session);
      createdNewSession = true;
      return session;
    }
  }

  public Map<String,String> params() {
    return params;
  }

  public void setSessionId(String sessionId) {
    this.sessionId = sessionId;
  }

  public String getSessionId() {
    return sessionId;
  }

  private String generateSessionId() {
    while (true) {
      StringBuilder sessionId = new StringBuilder(SESSION_ID_LENGTH);
      for (int i = 0; i < SESSION_ID_LENGTH; i++) {
        int idx = random.nextInt(CHARACTERS.length());
        sessionId.append(CHARACTERS.charAt(idx));
      }
      if (!sessionMap.containsKey(sessionId.toString())) {
        return sessionId.toString();
      }
    }

  }

  public boolean isCreatedNewSession() {
    return createdNewSession;
  }
}
