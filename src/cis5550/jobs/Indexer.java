package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.flame.FlameRDD;
import cis5550.tools.Logger;

import java.text.Normalizer;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import static cis5550.tools.URLParser.parseURL;
public class Indexer {
    private static final Pattern TAG_PATTERN = Pattern.compile("<[^>]*>");
    private static final Pattern NON_ALPHANUM_PATTERN = Pattern.compile("[^a-zA-Z0-9\\s]");
    private static final String DELIMITER = " ZKLURLANDPAGESEPARATOR ";
    private static final Logger logger = Logger.getLogger(Indexer.class);

    public static void run(FlameContext flameContext, String[] args) throws Exception {
        FlameRDD rawData = flameContext.fromTable("pt-crawl", row -> {
            String hashKey = row.key();
            String url = row.get("url");
            if (url != null && url.contains("..")) {
                return null;
            }
            String page = processAndNormalizeText(row.get("page")).trim();
            if (row.get("page") == null || row.get("page").isEmpty() || row.get("page").equals(" ")) {
                return null;
            }
            String responseCode = row.get("responseCode");
            if (!responseCode.equals("200")) {
                return null;
            }
            String title = row.get("title") != null && !row.get("title").isEmpty() ? processAndNormalizeText(row.get("title")) : "EMPTYTITLE";
            String description = row.get("description") != null && !row.get("description").isEmpty() ? processAndNormalizeText(row.get("description")) : "EMPTYDESCRIPTION";

            return hashKey + DELIMITER + url + DELIMITER + title + DELIMITER + description + DELIMITER + page;
        });

        ContentIndexer.run(rawData);
//        AggregateIndexer.run(flameContext);
    }

    private static String processAndNormalizeText(String content) {
        if (content == null || content.isEmpty()) {
            return "";
        }
        String noHtml = TAG_PATTERN.matcher(content).replaceAll("");
        String alphanumeric = NON_ALPHANUM_PATTERN.matcher(noHtml).replaceAll(" ");
        String lowerCase = alphanumeric.toLowerCase();
        String normalized = Normalizer.normalize(lowerCase, Normalizer.Form.NFD);
        return normalized.replaceAll("\\p{Mn}+", "");
    }
}
