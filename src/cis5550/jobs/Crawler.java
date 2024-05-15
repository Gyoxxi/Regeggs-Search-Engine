package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.flame.FlameRDD;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;
import cis5550.tools.Logger;
import cis5550.tools.URLParser;

import javax.net.ssl.SSLException;
import java.io.IOException;
import java.io.InputStream;
import java.net.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Crawler {

    private static final Logger LOGGER = Logger.getLogger(Crawler.class);
    private static final String CRAWLER_USER_AGENT = "cis5550-crawler";
    private static final String CRAW_TABLE_NAME = "pt-crawl";
    private static final String FRONTIER_TABLE_NAME = "pt-frontier";
    private static final String HOSTS_TABLE_NAME = "pt-hosts";
    private static final String DOMAIN_TABLE_NAME = "pt-domains";
    private static final String DISTRIBUTION_BLACKLIST_TABLE_NAME = "distribution-blacklist";
    private static final byte[] KVS_EMPTY_VALUE = " ".getBytes();
    private static final Set<String> FILE_EXTENSIONS_TO_EXCLUDE =
            new HashSet<>(Arrays.asList(".jpg", ".jpeg", ".gif", ".png", ".txt"));
    private static final Set<Integer> SUPPORTED_REDIRECT_RESPONSE_CODES =
            new HashSet<>(Arrays.asList(301, 302, 303, 307, 308));
    private static final Set<String> SUPPORTED_TOP_LEVEL_DOMAINS =
            new HashSet<>(Arrays.asList(".com", ".org", ".net", ".int", ".edu", ".gov"));
    private static final int RATE_LIMIT_INTERVAL_IN_MS = 1000;
    private static final int REQUEST_TIMEOUT_IN_MS = 10000;
    private static final int DISTRIBUTION_CALC_INTERVAL_IN_MS = 30 * 60 * 1000;
    private static final int CRAWL_TIME_LIMIT_IN_S = 300;
    private static final int DISTRIBUTION_PERCENT_THRESHOLD = 10;

    public static void run(FlameContext flameContext, String[] args) throws Exception {
        if (args.length < 1) {
            flameContext.output("Syntax: <seedUrl> [args...]\n");
        } else {
            flameContext.output("OK\n");
        }

        startDistributionCalcThread();

        String blacklistTable = args.length > 1 ? args[1] : null;

        String kvsCoordinator = flameContext.getKVS().getCoordinator();

        FlameRDD urlQueue;
        if (flameContext.getKVS().count(FRONTIER_TABLE_NAME) > 0) {
            urlQueue = flameContext.fromTable(FRONTIER_TABLE_NAME, (row) -> {
                KVSClient kvsClient = new KVSClient(kvsCoordinator);
                String urlString = row.get("value");
                String hashedUrl = Hasher.hash(urlString);
                try {
                    if (kvsClient.existsRow(CRAW_TABLE_NAME, hashedUrl)) {
                        return null;
                    }
                } catch (IOException ignored) {
                }
                return urlString;
            });
        } else {
            String seedUrl = args[0];
            String normSeedUrl = normalizeUrl(seedUrl, seedUrl);
            urlQueue = flameContext.parallelize(Collections.singletonList(normSeedUrl));
        }

        while (urlQueue.count() > 0) {
            FlameRDD newUrlQueue = urlQueue.flatMap(urlString -> {
                Iterable<String> nextUrls = new HashSet<>();
                ExecutorService executorService = Executors.newSingleThreadExecutor();
                Future<Iterable<String>> task = executorService.submit(() -> {
                    KVSClient kvsClient = new KVSClient(kvsCoordinator);
                    String hashedUrl = Hasher.hash(urlString);
                    String hostName = getHostName(urlString);
                    String hashedHostName = Hasher.hash(hostName);

                    if (kvsClient.existsRow(CRAW_TABLE_NAME, hashedUrl)) {
                        return new HashSet<>();
                    }

                    String domainName = getDomainName(getHostName(urlString));
                    String hashedDomainName = Hasher.hash(domainName);
                    if (kvsClient.existsRow(DISTRIBUTION_BLACKLIST_TABLE_NAME, hashedDomainName)) {
                        return new HashSet<>(List.of(urlString));
                    }

                    if (kvsClient.existsRow(HOSTS_TABLE_NAME, hashedHostName)) {
                        long lastAccessedTime = Long.parseLong(kvsClient.getRow(HOSTS_TABLE_NAME, hashedHostName)
                                .get("lastAccessedTime"));
                        int crawlDelayInMs = getCrawlDelayInMs(kvsClient, hashedHostName);
                        int rateLimitTimeIntervalInMs = crawlDelayInMs != -1 ? crawlDelayInMs : RATE_LIMIT_INTERVAL_IN_MS;
                        if (System.currentTimeMillis() - lastAccessedTime < rateLimitTimeIntervalInMs) {
                            return new HashSet<>(List.of(urlString));
                        }
                    }

                    if (!kvsClient.existsRow(HOSTS_TABLE_NAME, hashedHostName)) {
                        try {
                            handleGetRobots(kvsClient, hostName);
                        } catch (Exception e) {
                            if (isIgnorableException(e)) {
                                LOGGER.error(e.getMessage());
                                return new HashSet<>();
                            }
                            LOGGER.fatal(e.getMessage(), e);
                            throw new Exception(e);
                        }
                    }

                    if (blacklistTable != null && isBlacklistedUrl(kvsClient, blacklistTable, urlString)) {
                        return new HashSet<>();
                    }

                    if (isDisallowedUrl(kvsClient, hashedHostName, urlString)) {
                        return new HashSet<>();
                    }

                    HttpURLConnection headConnection;
                    try {
                        headConnection = handleHeadRequest(kvsClient, hostName, urlString);
                    } catch (Exception e) {
                        if (isIgnorableException(e)) {
                            LOGGER.error(e.getMessage());
                            return new HashSet<>();
                        }
                        LOGGER.fatal(e.getMessage(), e);
                        throw new Exception(e);
                    }

                    int headResponseCode = headConnection.getResponseCode();
                    String headContentType = headConnection.getContentType();

                    if (headResponseCode != 200 && !SUPPORTED_REDIRECT_RESPONSE_CODES.contains(headResponseCode)) {
                        return new HashSet<>();
                    }

                    if (SUPPORTED_REDIRECT_RESPONSE_CODES.contains(headResponseCode)) {
                        String redirectUrl = headConnection.getHeaderField("Location");
                        String normRedirectUrl;
                        try {
                            normRedirectUrl = normalizeUrl(urlString, redirectUrl);
                        } catch (Exception e) {
                            return new HashSet<>();
                        }
                        return new HashSet<>(List.of(normRedirectUrl));
                    }

                    if (headResponseCode != 200 || headContentType != null && !headContentType.toLowerCase().contains("text/html")) {
                        return new HashSet<>();
                    }

                    HttpURLConnection getConnection;
                    try {
                        getConnection = handleGetRequest(kvsClient, hostName, urlString);
                    } catch (Exception e) {
                        if (isIgnorableException(e)) {
                            LOGGER.error(e.getMessage());
                            return new HashSet<>();
                        }
                        LOGGER.fatal(e.getMessage(), e);
                        throw new Exception(e);
                    }

                    int getResponseCode = getConnection.getResponseCode();
                    String getContentType = getConnection.getContentType();
                    int getContentLength = getConnection.getContentLength();

                    if (getResponseCode == 200) {
                        InputStream inputStream = getConnection.getInputStream();
                        byte[] responseBody = inputStream.readAllBytes();
                        inputStream.close();

                        if (!isEnglishContent(responseBody)) {
                            return new HashSet<>();
                        }

                        byte[] documentTitle = getDocumentTitle(responseBody);
                        byte[] documentDescription = getDocumentDescription(responseBody);
                        byte[] sanitizedPage = sanitizeBody(responseBody);

                        Row row = new Row(hashedUrl);
                        row.put("contentType", getContentType);
                        row.put("isEnglish", "true");
                        row.put("length", String.valueOf(getContentLength));
                        row.put("page", sanitizedPage);
                        row.put("responseCode", String.valueOf(getResponseCode));
                        row.put("url", urlString);
                        row.put("description", documentDescription);
                        row.put("title", documentTitle);
                        kvsClient.putRow(CRAW_TABLE_NAME, row);

                        String getDomainName = getDomainName(hostName);
                        String getHashedDomainName = Hasher.hash(getDomainName);
                        String getPagesCrawled = new String(kvsClient.get(DOMAIN_TABLE_NAME, getHashedDomainName, "pagesCrawled"));
                        int getCurPagesCrawled = Integer.parseInt(getPagesCrawled);
                        Row domainRow = new Row(getHashedDomainName);
                        domainRow.put("domainName", getDomainName);
                        domainRow.put("pagesCrawled", String.valueOf(getCurPagesCrawled + 1));
                        kvsClient.putRow(DOMAIN_TABLE_NAME, domainRow);

                        return extractUrls(urlString, responseBody);
                    }

                    return new HashSet<>();
                });
                try {
                    nextUrls = task.get(CRAWL_TIME_LIMIT_IN_S, TimeUnit.SECONDS);
                } catch (Exception e) {
                    LOGGER.error(e.getMessage());
                } finally {
                    executorService.shutdownNow();
                }

                return nextUrls;
            });

            urlQueue.destroy();
            urlQueue = newUrlQueue;
            urlQueue.saveAsTable(FRONTIER_TABLE_NAME);
        }
    }

    public static void handleGetRobots(KVSClient kvsClient, String hostName) throws Exception {
        URL url = new URL(hostName + "/robots.txt");
        LOGGER.info("GET ... " + url);

        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("GET");
        connection.setConnectTimeout(REQUEST_TIMEOUT_IN_MS);
        connection.setReadTimeout(REQUEST_TIMEOUT_IN_MS);
        connection.setRequestProperty("User-Agent", CRAWLER_USER_AGENT);
        connection.connect();

        int responseCode = connection.getResponseCode();
        LOGGER.info("GET " + responseCode + " " + url);
        byte[] responseBody = KVS_EMPTY_VALUE;
        if (responseCode == 200) {
            InputStream inputStream = connection.getInputStream();
            responseBody = inputStream.readAllBytes();
            inputStream.close();
        }

        String hashedHostName = Hasher.hash(hostName);
        Row row = new Row(hashedHostName);
        row.put("hostName", hostName);
        row.put("page", responseBody);
        row.put("lastAccessedTime", String.valueOf(System.currentTimeMillis()));
        kvsClient.putRow(HOSTS_TABLE_NAME, row);


        String domainName = getDomainName(hostName);
        String hashedDomainName = Hasher.hash(domainName);
        if (!kvsClient.existsRow(DOMAIN_TABLE_NAME, hashedDomainName)) {
            Row domainRow = new Row(hashedDomainName);
            domainRow.put("domainName", domainName);
            domainRow.put("pagesCrawled", "0");
            kvsClient.putRow(DOMAIN_TABLE_NAME, domainRow);
        }

        connection.disconnect();
    }

    public static HttpURLConnection handleHeadRequest(
            KVSClient kvsClient, String hostName, String urlString) throws Exception {
        URL url = new URL(urlString);
        LOGGER.info("HEAD ... " + url);

        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("HEAD");
        connection.setConnectTimeout(REQUEST_TIMEOUT_IN_MS);
        connection.setReadTimeout(REQUEST_TIMEOUT_IN_MS);
        connection.setRequestProperty("User-Agent", CRAWLER_USER_AGENT);
        connection.setInstanceFollowRedirects(false);
        connection.connect();

        int responseCode = connection.getResponseCode();
        String contentType = connection.getContentType();
        int contentLength = connection.getContentLength();
        LOGGER.info("HEAD " + responseCode + " " + url);

        String hashedUrl = Hasher.hash(urlString);
        Row urlRow = new Row(hashedUrl);
        urlRow.put("contentType", contentType != null ? contentType : new String(KVS_EMPTY_VALUE));
        urlRow.put("isEnglish", KVS_EMPTY_VALUE);
        urlRow.put("length", String.valueOf(contentLength));
        urlRow.put("page", KVS_EMPTY_VALUE);
        urlRow.put("responseCode", String.valueOf(responseCode));
        urlRow.put("url", urlString);
        urlRow.put("description", KVS_EMPTY_VALUE);
        urlRow.put("title", KVS_EMPTY_VALUE);
        kvsClient.putRow(CRAW_TABLE_NAME, urlRow);

        String hashedHostName = Hasher.hash(hostName);
        kvsClient.put(HOSTS_TABLE_NAME, hashedHostName, "lastAccessedTime",
                String.valueOf(System.currentTimeMillis()));

        return connection;
    }

    public static HttpURLConnection handleGetRequest(
            KVSClient kvsClient, String hostName, String urlString) throws Exception {
        URL url = new URL(urlString);
        LOGGER.info("GET ... " + url);

        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("GET");
        connection.setConnectTimeout(REQUEST_TIMEOUT_IN_MS);
        connection.setReadTimeout(REQUEST_TIMEOUT_IN_MS);
        connection.setRequestProperty("User-Agent", CRAWLER_USER_AGENT);
        connection.connect();

        int responseCode = connection.getResponseCode();
        LOGGER.info("GET " + responseCode + " " + url);

        String hashedUrl = Hasher.hash(urlString);
        kvsClient.put(CRAW_TABLE_NAME, hashedUrl, "responseCode", String.valueOf(responseCode));

        String hashedHostName = Hasher.hash(hostName);
        kvsClient.put(HOSTS_TABLE_NAME, hashedHostName, "lastAccessedTime",
                String.valueOf(System.currentTimeMillis()));

        return connection;
    }

    public static Set<String> extractUrls(String pageBaseUrl, byte[] page) {
        Set<String> urls = new HashSet<>();
        String htmlElement = new String(page);

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
            boolean shouldFilterUrl = false;
            try {
                shouldFilterUrl = shouldFilterUrl(normUrl);
            } catch (Exception e) {
            }

            if (shouldFilterUrl) {
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

    public static String getHostName(String url) {
        String[] UrlTokens = URLParser.parseURL(url);
        String protocol = UrlTokens[0];
        String domain = UrlTokens[1];
        return protocol + "://" + domain;
    }

    public static boolean shouldFilterUrl(String url) throws Exception {
        Random rand = new Random();
        int randInt = rand.nextInt(3);
        if (randInt == 0) {
            return true;
        }

        String[] urlTokens = URLParser.parseURL(url);
        String protocol = urlTokens[0];
        String filePath = urlTokens[3];

        if (!protocol.equals("http") && !protocol.equals("https")) {
            return true;
        }

        if (filePath.split("/").length > 4) {
            return true;
        }

        if (filePath.contains("contact") || filePath.contains("signin") || filePath.contains("login")
                || filePath.contains("signup") || filePath.contains("logout") || filePath.contains("signout")
                || filePath.contains("support")) {
            return true;
        }

        int extensionStartIdx = filePath.lastIndexOf('.');
        if (extensionStartIdx != -1) {
            String fileExtension = filePath.substring(extensionStartIdx);
            if (FILE_EXTENSIONS_TO_EXCLUDE.contains(fileExtension.toLowerCase())) {
                return true;
            }
        }

        if (url.contains("wiki") && !isAcceptedWikiLanguage(url)) {
            return true;
        }

        if (url.contains("archive.org") || url.contains("wiktionary")) {
            return true;
        }

        return !isAcceptedTopLevelDomain(url);
    }

    public static boolean isDisallowedUrl(
            KVSClient kvsClient, String hashedHostName, String normUrl) throws IOException {
        String body = new String(kvsClient.get(HOSTS_TABLE_NAME, hashedHostName, "page"));
        if (body.isBlank()) {
            return false;
        }

        List<String[]> targetUserGroups = getTargetUserGroups(body);

        for (String[] userGroup : targetUserGroups) {
            String filePath = URLParser.parseURL(normUrl)[3];

            for (String content : userGroup) {
                Pattern allowPattern = Pattern.compile("Allow:\\s+(.+)\\s*");
                Matcher allowMatcher = allowPattern.matcher(content);
                while (allowMatcher.find()) {
                    String allowedFilePath = allowMatcher.group(1);
                    if (filePath.startsWith(allowedFilePath)) {
                        return false;
                    }
                }

                Pattern disallowPattern = Pattern.compile("Disallow:\\s+(.+)\\s*");
                Matcher disallowMatcher = disallowPattern.matcher(content);
                while (disallowMatcher.find()) {
                    String disallowedFilePath = disallowMatcher.group(1);
                    if (filePath.startsWith(disallowedFilePath)) {
                        return true;
                    }
                }
            }
        }

        return false;
    }

    public static int getCrawlDelayInMs(KVSClient kvsClient, String hashedHostName) throws IOException {
        String body = new String(kvsClient.get(HOSTS_TABLE_NAME, hashedHostName, "page"));
        if (body.isBlank()) {
            return -1;
        }

        List<String[]> targetUserGroups = getTargetUserGroups(body);

        for (String[] userGroup : targetUserGroups) {
            for (String content : userGroup) {
                Pattern pattern = Pattern.compile("Crawl-delay:\\s+(.+)\\s*");
                Matcher matcher = pattern.matcher(content);
                if (matcher.find()) {
                    try {
                        float crawlDelayInSec = Float.parseFloat(matcher.group(1));
                        return Math.round(crawlDelayInSec * 1000);
                    } catch (NumberFormatException e) {
                        return -1;
                    }
                }
            }
        }

        return -1;
    }

    public static List<String[]> getTargetUserGroups(String body) {
        List<String[]> userGroups = new ArrayList<>();

        if (!body.contains("User-agent")) {
            return userGroups;
        }

        int startIdx = 0;
        while (startIdx != -1) {
            Map.Entry<Integer, String> tuple = extractUserGroup(body, startIdx);
            startIdx = tuple.getKey();
            String userGroup = tuple.getValue();

            if (userGroup.isEmpty() || userGroup.isBlank()) {
                continue;
            }

            String[] tokens = userGroup.split("\n");
            String[] content = Arrays.stream(tokens).map(String::trim).toArray(String[]::new);

            String userAgentContent = content[0];
            if (isTargetUserAgent(userAgentContent)) {
                userGroups.add(content);
            }
        }

        return userGroups;
    }

    public static Map.Entry<Integer, String> extractUserGroup(String body, int startIdx) {
        String userGroup = "";

        int endIdx = body.indexOf("User-agent:", startIdx);
        if (startIdx == 0) {
            // skip first occurrence
            startIdx = endIdx + 1;
        } else if (endIdx == -1) {
            // handle last occurrence
            userGroup = body.substring(startIdx - 1);
            startIdx = endIdx;
        } else {
            userGroup = body.substring(startIdx - 1, endIdx);
            startIdx = endIdx + 1;
        }

        return new AbstractMap.SimpleEntry<>(startIdx, userGroup);
    }

    public static boolean isTargetUserAgent(String s) {
        Pattern pattern = Pattern.compile("User-agent:\\s+(.+)\\s*");
        Matcher matcher = pattern.matcher(s);
        if (!matcher.find()) {
            return false;
        }
        String userAgent = matcher.group(1);
        return userAgent.equals("*") || userAgent.equals(CRAWLER_USER_AGENT);
    }

    public static boolean isBlacklistedUrl(
            KVSClient kvsClient, String blacklistTable, String normUrl) throws IOException {
        Iterator<Row> it = kvsClient.scan(blacklistTable);
        while (it.hasNext()) {
            Row row = it.next();
            String patternStr = row.get("pattern").replaceAll("\\*", ".*");
            Pattern pattern = Pattern.compile(patternStr);
            Matcher matcher = pattern.matcher(normUrl);

            if (matcher.find()) {
                String curMatchUrls = row.get("matchedUrls");
                String newMatchUrls = (curMatchUrls != null ? curMatchUrls + ",\n" : "") + normUrl;
                kvsClient.put(blacklistTable, row.key(), "matchedUrls", newMatchUrls);
                return true;
            }
        }

        return false;
    }

    public static boolean isEnglishContent(byte[] page) {
        String htmlElement = new String(page);
        String htmlTagWithLangAttributePattern = "<html\\s+[^>]*lang=[\"']([^\"']*)[\"'][^>]*>";
        Pattern pattern = Pattern.compile(htmlTagWithLangAttributePattern, Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(htmlElement);
        if (matcher.find()) {
            String language = matcher.group(1);
            return language.equalsIgnoreCase("en");
        }

        return false;
    }

    public static byte[] getDocumentTitle(byte[] page) {
        String htmlElement = new String(page);
        String TitleTag = "<title[^>]*>(.*?)</title>";
        Pattern pattern = Pattern.compile(TitleTag, Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(htmlElement);
        if (matcher.find()) {
            String title = matcher.group(1);
            return title.getBytes();
        }

        return KVS_EMPTY_VALUE;
    }

    public static byte[] getDocumentDescription(byte[] page) {
        String htmlElement = new String(page);
        String descriptionTagPattern = "<meta[^>]*\\s+name\\s*=\\s*[\"']description[\"']\\s+content\\s*=\\s*[\"'](.*?)[\"']\\s*/?>";
        Pattern pattern = Pattern.compile(descriptionTagPattern, Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(htmlElement);
        if (matcher.find()) {
            String description = matcher.group(1);
            return description.getBytes();
        }

        return KVS_EMPTY_VALUE;
    }

    public static byte[] sanitizeBody(byte[] page) {
        String document = new String(page);
        String bodyTag = "<body[^>]*>(.*?)</body>";
        Pattern pattern = Pattern.compile(bodyTag, Pattern.DOTALL);
        Matcher matcher = pattern.matcher(document);
        if (matcher.find()) {
            String sanitizedDoc = matcher.group(1)
                    .replaceAll("[\\n\\r]+", " ")
                    .replaceAll("<!--.*?-->", "")
                    .replaceAll("<script[^>]*>.*?</script>", "")
                    .replaceAll("<style[^>]*>.*?</style>", "")
                    .replaceAll("<(?!(a\\s+(?:[^>]*?\\s+)?href=\"([^\"]*)\")|(/a\\s*(?:[^>]*?\\s+)?))[^>]*>", " ");
            return sanitizedDoc.getBytes();
        }

        return KVS_EMPTY_VALUE;
    }

    public static boolean isIgnorableException(Exception e) {
        return e instanceof SSLException || e instanceof SocketTimeoutException
                || e instanceof UnknownHostException || e instanceof SocketException
                || e instanceof ProtocolException || e instanceof MalformedURLException;
    }

    public static boolean isAcceptedWikiLanguage(String normUrl) {
        // had to create a method for Wikipedia because the crawler encounters
        // a lot of pages from Wikipedia that are non-English
        // for non-Wikipedia pages, non-English pages will be stored in pt-crawl
        // but not parsed, thus contain no content in "page" column

        Pattern pattern = Pattern.compile("https://([a-z]{2})\\.wiki[a-z]*\\.");
        Matcher matcher = pattern.matcher(normUrl);
        if (matcher.find()) {
            String region = matcher.group(1);
            return region.equalsIgnoreCase("en");
        }

        return false;
    }

    public static boolean isAcceptedTopLevelDomain(String normUrl) {
        for (String topLevelDomain : SUPPORTED_TOP_LEVEL_DOMAINS) {
            if (normUrl.contains(topLevelDomain)) {
                return true;
            }
        }

        return false;
    }

    public static String getDomainName(String url) throws URISyntaxException {
        URI uri = new URI(url);
        String domain = uri.getHost();
        domain = domain.startsWith("www.") ? domain.substring(4) : domain;
        String[] domainTokens = domain.split("\\.");
        return domainTokens[domainTokens.length - 2] + "." + domainTokens[domainTokens.length - 1];
    }

    public static void startDistributionCalcThread() {
        Thread thread = new Thread(() -> {
            while (true) {
                KVSClient kvsClient = new KVSClient("localhost:8000");
                try {
                    int totalCrawledPages = kvsClient.count(CRAW_TABLE_NAME);
                    if (totalCrawledPages == 0) {
                        continue;
                    }
                    LOGGER.info("totalCrawledPages: " + totalCrawledPages);

                    Iterator<Row> iter = kvsClient.scan(DOMAIN_TABLE_NAME);
                    while (iter.hasNext()) {
                        Row domainRow = iter.next();
                        String rowKey = domainRow.key();
                        String domainName = domainRow.get("domainName");
                        int pagesCrawled = Integer.parseInt(domainRow.get("pagesCrawled"));

                        double percentShare = (double) pagesCrawled / totalCrawledPages * 100;
                        boolean isWikipedia = domainName.contains("wikipedia");
                        if ((!isWikipedia && percentShare > DISTRIBUTION_PERCENT_THRESHOLD) || percentShare > 30) {
                            Row distRow = new Row(rowKey);
                            distRow.put("domainName", domainName);
                            distRow.put("percentShare", String.valueOf(percentShare));
                            kvsClient.putRow(DISTRIBUTION_BLACKLIST_TABLE_NAME, distRow);
                        }
                    }
                } catch (Exception e) {
                    LOGGER.error(e.getMessage());
                }

                try {
                    Thread.sleep(DISTRIBUTION_CALC_INTERVAL_IN_MS);
                } catch (InterruptedException ignored) {
                }
            }
        });
        thread.start();
    }

    public static void main(String[] args) throws Exception {
        KVSClient kvs = new KVSClient("localhost:8000");

        int tableSize = kvs.count(CRAW_TABLE_NAME);
        System.out.println("tableSize: " + tableSize);

        int nonEmptyRows = 0;
        Map<String, Integer> domainCounter = new HashMap<>();
        Map<String, Integer> emptyCounter = new HashMap<>();
        Iterator<Row> iter = kvs.scan(CRAW_TABLE_NAME);
        while (iter.hasNext()) {
            Row row = iter.next();
            String page = row.get("page");
            String url = row.get("url");
            String responseCode = row.get("responseCode");

            if (page.equals(new String(KVS_EMPTY_VALUE))) {
                if (!emptyCounter.containsKey(responseCode)) {
                    emptyCounter.put(responseCode, 0);
                }
                emptyCounter.put(responseCode, emptyCounter.get(responseCode) + 1);
                continue;
            }
            nonEmptyRows++;

            String domainName = getDomainName(getHostName(url));
            if (!domainCounter.containsKey(domainName)) {
                domainCounter.put(domainName, 0);
            }
            domainCounter.put(domainName, domainCounter.get(domainName) + 1);
        }

        System.out.println("nonEmptyRows: " + nonEmptyRows);

        Map<String, Double> domainPercentages = new HashMap<>();
        for (String domainName : domainCounter.keySet()) {
            domainPercentages.put(domainName, (double) domainCounter.get(domainName) / nonEmptyRows * 100);
        }

        Map<Double, List<String>> domainDistribution = new TreeMap<>();
        for (String domainName : domainPercentages.keySet()) {
            Double percent = Math.round(domainPercentages.get(domainName) * 100.0) / 100.0;
            if (!domainDistribution.containsKey(percent)) {
                domainDistribution.put(percent, new ArrayList<>());
            }
            domainDistribution.get(percent).add(domainName);
        }

        System.out.println("{");
        for (Double percent : domainDistribution.keySet()) {
            System.out.println("   " + percent + "%: " + domainDistribution.get(percent));
        }
        System.out.println("}");

        Map<Integer, List<String>> emptyDistribution = new TreeMap<>();
        for (String responseCode : emptyCounter.keySet()) {
            int count = emptyCounter.get(responseCode);
            if (!emptyDistribution.containsKey(count)) {
                emptyDistribution.put(count, new ArrayList<>());
            }
            emptyDistribution.get(count).add(responseCode);
        }

        System.out.println("{");
        for (Integer count : emptyDistribution.keySet()) {
            System.out.println("   " + count + ": " + emptyDistribution.get(count));
        }
        System.out.println("}");
    }
}
