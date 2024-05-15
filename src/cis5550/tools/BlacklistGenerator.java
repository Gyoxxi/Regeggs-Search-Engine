package cis5550.tools;

import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;

public class BlacklistGenerator {

    private static final String WORKER_URL = "http://localhost:8001/data";
    private static final String BLACKLIST_TABLE_NAME = "pt-blacklist";
    private static final String[] PATTERNS = {"http*://arxiv.org*"};

    public static void main(String[] args) throws Exception {
        for (String pattern : PATTERNS) {
            StringBuilder urlBuilder = new StringBuilder(WORKER_URL);
            String hashedPattern = Hasher.hash(pattern);
            urlBuilder.append("/").append(BLACKLIST_TABLE_NAME);
            urlBuilder.append("/").append(hashedPattern);
            urlBuilder.append("/").append("pattern");

            URL url = new URL(urlBuilder.toString());
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("PUT");
            connection.setDoOutput(true);
            OutputStreamWriter writer = new OutputStreamWriter(connection.getOutputStream());
            writer.write(pattern);
            writer.flush();
            writer.close();

            connection.connect();

            if (connection.getResponseCode() != 200) {
                System.out.println("[SUCCESS] PUT " + connection.getURL());
            } else {
                System.out.println("[FAILED] PUT " + connection.getURL() + " with code " + connection.getResponseCode());
            }
        }
    }

}
