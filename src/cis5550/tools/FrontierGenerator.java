package cis5550.tools;

import cis5550.jobs.Crawler;

import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;

public class FrontierGenerator {

    private static final String WORKER_URL = "http://localhost:8001/data";
    private static final String FRONTIER_TABLE_NAME = "pt-frontier";
    private static final String[] SEED_URLS = {
            "https://philly.eater.com/maps/38-best-philadelphia-restaurants",
            "https://www.cnn.com",
            "https://www.nytimes.com",
            "https://nature.com",
            "https://stackoverflow.com",
            "https://www.imdb.com",
            "https://ocw.mit.edu/",
            "https://en.wikipedia.org/wiki/Main_Page",
            "https://en.wikipedia.org/wiki/Film",
            "https://en.wikipedia.org/wiki/Sport",
            "https://en.wikipedia.org/wiki/Statistics",
            "https://en.wikipedia.org/wiki/Computer_science",
            "https://www.bbc.com/news",
            "https://www.espn.com/",
            "https://www.rottentomatoes.com/",
            "https://www.booking.com/",
            "https://www.expedia.com",
            "https://www.visittheusa.com/",
            "https://www.usa.gov/"
    };

    public static void main(String[] args) throws Exception {
        for (String seedUrl : SEED_URLS) {
            String normUrl = Crawler.normalizeUrl(seedUrl, seedUrl);

            StringBuilder urlBuilder = new StringBuilder(WORKER_URL);
            String hashedUrl = Hasher.hash(normUrl);
            urlBuilder.append("/").append(FRONTIER_TABLE_NAME);
            urlBuilder.append("/").append(hashedUrl);
            urlBuilder.append("/").append("value");

            URL url = new URL(urlBuilder.toString());
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setDoOutput(true);
            connection.setRequestMethod("PUT");
            OutputStream outputStream = connection.getOutputStream();
            OutputStreamWriter writer = new OutputStreamWriter(outputStream);
            writer.write(normUrl);
            writer.flush();
            writer.close();
            outputStream.close();
            connection.connect();

            if (connection.getResponseCode() != 200) {
                System.out.println("[SUCCESS] PUT " + connection.getURL());
            } else {
                System.out.println("[FAILED] PUT " + connection.getURL() + " with code " + connection.getResponseCode());
            }
        }
    }
}
