package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlameRDD;
import cis5550.tools.Hasher;
import cis5550.tools.Logger;
import cis5550.tools.URLParser;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PageRank {

    private static final String URL_DELIMITER = "URLANDPAGESEAPARATOR";
    private static final String LINK_DELIMITER = " URLSEPARATOR ";
    private static final int DELIMITER_SIZE = LINK_DELIMITER.length();
    private static int iteration = 1;
    private static final Logger logger = Logger.getLogger(PageRank.class);

    public static void run(FlameContext flameContext, String[] args) throws Exception {

        if (args.length == 0) {
            flameContext.output("Error: no convergence threshold provided!");
            return;
        }

        double convergenceThreshold = Double.parseDouble(args[0]);

        FlameRDD ptCrawlData = flameContext.fromTable("pt-crawl", row -> {
            String url = row.get("url");
            if (url != null && url.contains("..")) {
                return null;
            }
            String page = row.get("page");
            if (row.get("page") == null || row.get("page").isEmpty() || row.get("page").equals(" ")) {
                return null;
            }
            String responseCode = row.get("responseCode");
            if (!responseCode.equals("200")) {
                return null;
            }
            String rowKey = row.key();
            return rowKey + URL_DELIMITER + url + URL_DELIMITER + page;
        });

        FlamePairRDD indexedData = ptCrawlData.mapToPair(line -> {
            if (line == null || line.isEmpty()) {
                return null;
            }
            String[] parts = line.split(URL_DELIMITER, 2);
            return new FlamePair(parts[0], parts[1]);
        });

        ptCrawlData.destroy();

        FlamePairRDD stateTable = indexedData.flatMapToPair(flamePair -> {
            if (flamePair == null) {
                return new ArrayList<>();
            }

            String rowKey = flamePair._1();
            String urlAndPage = flamePair._2();
            String[] parts = urlAndPage.split(URL_DELIMITER, 2);
            String url = parts[0];
            String pageContent = parts[1] != null && !parts[1].isEmpty() ? parts[1] : "";

            if (url.contains("web.archive.org")) {
                pageContent = "";
            }

            Set<String> normalizedUrls = extractUrls(url, pageContent);
            Set<String> links = new HashSet<>();
            for (String normalized : normalizedUrls) {
                if (normalized != null && !normalized.isEmpty()) {
                    links.add(normalized);
                }
            }

            String allLinks = String.join(LINK_DELIMITER, links);

            return Collections.singletonList(new FlamePair(rowKey, "1.0" + LINK_DELIMITER + "1.0" + LINK_DELIMITER + allLinks));
        });

        indexedData.destroy();

        boolean converged = false;
        double decayFactor = 0.85;

        while (!converged) {
            logger.info("Current iteration: " + iteration);

            FlamePairRDD transferTable = stateTable.flatMapToPair(statePair -> {
                String rowKey = statePair._1();
                String s = statePair._2();
                int comma1 = s.indexOf(LINK_DELIMITER);
                int comma2 = s.indexOf(LINK_DELIMITER, comma1 + DELIMITER_SIZE);

                String curRank = s.substring(0, comma1);
                Set<String> links = new HashSet<>(Arrays.asList(s.substring(comma2 + DELIMITER_SIZE).split(LINK_DELIMITER)));

                Set<FlamePair> transfers = new HashSet<>();
                double transferValue = !links.isEmpty() ? decayFactor * Double.parseDouble(curRank) / links.size() : 0.0;

                for (String link : links) {
                    if (link != null && !link.isEmpty()) {
                        transfers.add(new FlamePair(Hasher.hash(link), String.valueOf(transferValue)));
                    }
                }
                transfers.add(new FlamePair(rowKey, String.valueOf(0.0)));
                return transfers;
            });

            FlamePairRDD aggregatedTable = transferTable.foldByKey("0.0", (sum, vi) -> {
                if (sum == null || sum.isEmpty()) {
                    return vi;
                }
                return String.valueOf(Double.parseDouble(sum) + Double.parseDouble(vi));
            });

            transferTable.destroy();

            FlamePairRDD joinedTable = stateTable.join(aggregatedTable);

            stateTable.destroy();
            aggregatedTable.destroy();

            FlamePairRDD updatedStateTable = joinedTable.flatMapToPair(joinedPair -> {
                String rowKey = joinedPair._1();
                String s = joinedPair._2();
                int comma1 = s.indexOf(LINK_DELIMITER);
                int comma2 = s.indexOf(LINK_DELIMITER, comma1 + DELIMITER_SIZE);
                int comma3 = s.lastIndexOf(LINK_DELIMITER);

                String newPrevRank;
                String links;
                String newS = "";
                try {
                    newPrevRank = s.substring(0, comma1);
                    links = s.substring(comma2 + DELIMITER_SIZE, comma3);
                    String newCurrRank = String.valueOf(0.15 + Double.parseDouble(s.substring(comma3 + DELIMITER_SIZE)));
                    newS = newCurrRank + LINK_DELIMITER + newPrevRank + LINK_DELIMITER + links;
                } catch (Exception e) {
                    logger.warn("An error occurred but will continue: " + e.getMessage());
                    logger.info("cur url: " + rowKey);
                    logger.info("cur s: " + s);
                    logger.info("comma1, 2, 3: " + comma1 + ", " + comma2 + ", " + comma3);
                }

                return Collections.singletonList(new FlamePair(rowKey, newS));
            });

            joinedTable.destroy();

            FlameRDD maxChangeRdd = updatedStateTable.flatMap(flamePair -> {
                String s = flamePair._2();
                int comma1 = s.indexOf(LINK_DELIMITER);
                int comma2 = s.indexOf(LINK_DELIMITER, comma1 + DELIMITER_SIZE);

                String currRank = s.substring(0, comma1);
                String prevRank = s.substring(comma1 + DELIMITER_SIZE, comma2);

                double diff = Math.abs(Double.parseDouble(currRank) -
                        Double.parseDouble(prevRank));
                return Collections.singletonList(String.valueOf(diff));
            });

            String maxDiff = maxChangeRdd.fold("0.0", (maxChange, change) -> {
                if (maxChange == null || maxChange.isEmpty()) {
                    return change;
                }
                double maxChangeDouble = Double.parseDouble(maxChange);
                double changeDouble = Double.parseDouble(change);
                return String.valueOf(Math.max(maxChangeDouble, changeDouble));
            });

            maxChangeRdd.destroy();

            if (Double.parseDouble(maxDiff) < convergenceThreshold) {
                converged = true;
            }
            stateTable = updatedStateTable;
            iteration += 1;
        }

        stateTable.flatMapToPair(flamePair -> {

            String rowKey = flamePair._1();
            String s = flamePair._2();
            String curRank = s.split(LINK_DELIMITER)[0];
            flameContext.getKVS().put("pt-pageranks", rowKey, "rank", curRank);

            return Collections.emptyList();
        });

    }

    public static Set<String> extractUrls(String pageBaseUrl, String htmlElement) {
        Set<String> urls = new HashSet<>();

        String anchorTagWithHrefAttributePattern = "<a\\s+[^>]*href=[\"']([^\"']*)[\"'][^>]*>";
        Pattern pattern = Pattern.compile(anchorTagWithHrefAttributePattern, Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(htmlElement);
        while (matcher.find()) {
            String url = matcher.group(1);
            String normUrl;
            try {
                normUrl = normalizeUrl(pageBaseUrl, url);
            } catch (Exception e) {
                continue;
            }
            if (normUrl.matches("^(javascript|mailto|tel|ftp|file):.*")) {
                continue;
            }
            urls.add(normUrl);
        }

        return urls;
    }

    public static String normalizeUrl(String baseUrl, String parsedUrl) {
        StringBuilder normUrl = new StringBuilder();

        String[] parsedUrlTokens = URLParser.parseURL(parsedUrl);
        String parsedProtocol = parsedUrlTokens[0];
        String parsedDomain = parsedUrlTokens[1];
        String parsedPort = parsedUrlTokens[2];
        String parsedFilePath = parsedUrlTokens[3];

        String[] baseUrlTokens = URLParser.parseURL(baseUrl);
        String baseProtocol = baseUrlTokens[0];
        String baseDomain = baseUrlTokens[1];
        String basePort = baseUrlTokens[2];
        String baseFilePath = baseUrlTokens[3];

        String protocol = parsedProtocol != null ? parsedProtocol : baseProtocol;
        normUrl.append(protocol).append(":/");

        String domain = parsedDomain != null ? parsedDomain : baseDomain;
        normUrl.append("/").append(domain);

        String port;
        if (parsedPort != null) {
            port = parsedPort;
        } else if (basePort != null) {
            port = basePort;
        } else {
            port = baseProtocol.equals("http") ? "80" : "443";
        }
        normUrl.append(":").append(port);

        String filePath;
        String[] parsedFilePathTokens = parsedFilePath.split("#");
        parsedFilePath = parsedFilePathTokens.length > 0 ? parsedFilePath.split("#")[0] : "";
        if (parsedFilePath.isEmpty()) {
            filePath = baseFilePath;
        } else if (parsedFilePath.startsWith("/")) {
            filePath = parsedFilePath;
        } else {
            String baseFilePathBeforeFileName = baseFilePath.substring(0, baseFilePath.lastIndexOf('/'));
            String[] parsedTokens = parsedFilePath.split("/");
            for (String parsedToken : parsedTokens) {
                if (!parsedToken.equals("..")) {
                    continue;
                }
                baseFilePathBeforeFileName = baseFilePath.substring(0, baseFilePathBeforeFileName.lastIndexOf('/'));
                parsedFilePath = parsedFilePath.substring(3);
            }
            filePath = baseFilePathBeforeFileName + "/" + parsedFilePath;
        }
        normUrl.append(filePath);

        return normUrl.toString();
    }
}