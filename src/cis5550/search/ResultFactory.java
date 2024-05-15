package cis5550.search;

import cis5550.external.PorterStemmer;
import cis5550.flame.FlameSubmit;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;
import cis5550.tools.URLParser;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;

import static java.lang.Thread.sleep;
import static java.util.Locale.US;

public class ResultFactory {

    private static final String KVS_HOST = "54.196.3.207:8000";

    private static final KVSClient kvsClient = new KVSClient(KVS_HOST);

    private static List<List<SearchResults.Result>> results = new ArrayList<>();

    private static final int STEP = 10;

    private static final int SNIPPET_LEN = 130;

    private static final int TITLE_LEN = 56;

    private static final Set<String> querySet = new HashSet<>();
    private static StringBuilder queryTableName = new StringBuilder();

    private static final String SEPARATOR = "URLSEPARATOR";

    private static final Set<String> BLACKLIST = new HashSet<>(List.of("mejaxcxqvoacwgtakmvisgwibgrezkzovkagoayc"));

    private static boolean isValidTerm(String term) {
        // Check if the term is purely alphabetic or numeric
        if (term.matches("[a-zA-Z]+") || term.matches("\\d+")) {
            return true;
        }

        // Check for valid ordinal numbers (e.g., 1st, 2nd, 3rd, 4th, etc.)
        return term.matches("\\d+(st|nd|rd|th)");
    }

    public static List<String> processString(String input) {
        // Remove non-alphanumeric characters except spaces
        String cleanedInput = input.replaceAll("[^a-zA-Z0-9\\s]", "").toLowerCase();

        // Split the string into terms
        String[] terms = cleanedInput.split("\\s+");
        List<String> validTerms = new ArrayList<>();

        // Iterate through each term
        for (String term : terms) {
            if (isValidTerm(term)) {
                querySet.add(term);
                validTerms.add(term);
                queryTableName.append(term);
            }
        }
        return validTerms;
    }

    public static SearchResults createResults(String queryTerms, int offset) {
        // new query, refresh query set and query table name
        querySet.clear();
        queryTableName = new StringBuilder();
        try {
            // parse and populate query term set
            // jobInput contains parsed and separated, lowercase, alphanumeric terms
            List<String> jobInput = processString(queryTerms);
            HashMap<String, Double> ranks = processTotalRank(jobInput);

            // First time fetching data for the query
            if (offset == 0) {
                results.clear();
                // Get search results from table (sorted)
                splitIntoChunks(fetchResultsFromTable(ranks));
            }

            // Return nothing, when offset exceeds results size
            if (Math.floorDiv(offset, STEP) >= results.size()) {
                return new SearchResults(new ArrayList<>());
            }

            // Now placeholder text in sublist of results
            List<SearchResults.Result> output = results.get(Math.floorDiv(offset, STEP));


            // Update result details
            for (SearchResults.Result result : output) {
                String url = result.getUrl();
                String rowKey = result.getRowKey();
                updateSnippet(rowKey, result);
                updateTitle(rowKey, result);
                updateHostName(url, result);
                updateUrl(url, result);
            }

            return new SearchResults(output);
        } catch (Exception e) {
            e.printStackTrace();
            return new SearchResults(new ArrayList<>());
        }
    }

    private static void splitIntoChunks(
            ConcurrentSkipListSet<SearchResults.Result> fetchedSet) {
        ArrayList<SearchResults.Result> currentChunk = new ArrayList<>();

        int count = 0;
        for (SearchResults.Result res : fetchedSet) {
            currentChunk.add(res);
            count++;
            if (count == STEP) {
                results.add(currentChunk);
                currentChunk = new ArrayList<>();
                count = 0;
            }
        }

        if (!currentChunk.isEmpty()) {
            results.add(currentChunk);
        }
    }

    private  static void updateUrl(String url, SearchResults.Result result) {
        String[] parts = URLParser.parseURL(url);
        String parsed = parts[0] + "://" + parts[1] + parts[3];
        parsed = parsed.split(",")[0];
        result.setUrl(parsed);
    }

    private static void updateHostName(String url, SearchResults.Result result) {
        // Parse the URL and get the host part, converted to upper case
        String host = URLParser.parseURL(url)[1].toUpperCase();
        String[] parts = host.split("\\.");

        if (parts.length == 2) {
            result.setHostname(host);
        } else {
            result.setHostname(String.join(".", Arrays.copyOfRange(parts, 1, parts.length)));
        }
    }


    private static void updateTitle(String rowKey, SearchResults.Result result) throws IOException {
        byte[] titleBytes = kvsClient.get("pt-crawl", rowKey, "title");
        String title;
        if (titleBytes != null && titleBytes.length != 0) {
            title = new String(titleBytes,  StandardCharsets.UTF_8);
            if (title.trim().isEmpty()) {
                result.setTitle("No Title");
                return;
            }
            result.setTitle(title);
            return;
        }
        result.setTitle("No Title");
    }

    private static void updateSnippet(String rowKey, SearchResults.Result result) throws IOException {
        // Attempt to get the 'description' bytes first
        byte[] contentBytes = kvsClient.get("pt-crawl", rowKey, "page");

        // If page content is empty then no need to parse for snippet
        if (contentBytes == null || contentBytes.length == 0) {
            result.setSnippet("Snippet not available.");
            return;
        }

        String content = new String(contentBytes, StandardCharsets.UTF_8);
        SnippetGenerator sg = new SnippetGenerator();
        String snippet = sg.generateSnippet(content, querySet, SNIPPET_LEN);

        // Fallback to 'description' if terms not found in snippet
        if (!sg.isTermFound()) {
            contentBytes = kvsClient.get("pt-crawl", rowKey, "description");
            if (!(contentBytes == null || contentBytes.length == 0)) {
                content = new String(contentBytes, StandardCharsets.UTF_8);
                int len = Math.min(SNIPPET_LEN, content.length());
                String description = content.substring(0, len);

                // 'description' is not available, then use first part of page
                if (description.trim().isEmpty()) {
                    result.setSnippet(snippet);
                    return;
                }

                // Append ellipsis if content is longer than the snippet length
                if (content.length() > SNIPPET_LEN) {
                    description += " ...";
                } else if (!snippet.endsWith(".")) {
                    description += ".";
                }
                result.setSnippet(description);
                return;
            }
        }

        result.setSnippet(snippet);
    }


    private static ConcurrentSkipListSet<SearchResults.Result> fetchResultsFromTable(HashMap<String, Double> ranks)
            throws IOException {
        ConcurrentSkipListSet<SearchResults.Result> ret = new ConcurrentSkipListSet<>();

        for (String keyString : ranks.keySet()) {
            String urlHash = keyString.split("-url-")[0];
            String url = keyString.split("-url-")[1];
            if (BLACKLIST.contains(urlHash)) {
                continue;
            }

            double score = ranks.get(keyString);

            SearchResults.Result r = new SearchResults.Result(url,
                    "Example.com",
                    "Example Title",
                    "Example Snippet",
                    score,
                    urlHash
                    );
            ret.add(r);
        }

        return ret;
    }

    public static PagePreview createPagePreview(String rowKey) {
        try {
            // Fetch the page content using the rowKey from the KVS
            byte[] pageBytes = kvsClient.get("pt-crawl", rowKey, "page");

            // Handle the case where no page content was found
            if (pageBytes == null || pageBytes.length == 0) {
                return new PagePreview("Page content not available.");
            }

            // Convert byte array to String
            String pageContent = new String(pageBytes, StandardCharsets.UTF_8);
            return new PagePreview(pageContent);
        } catch (IOException e) {
            return new PagePreview("Unable to fetch page content.");
        }
    }


    private static HashMap<String, Double> processTotalRank(List<String> wordsIntermediate) throws IOException {
        String[] words = new String [wordsIntermediate.size()];
        for(int i = 0; i < wordsIntermediate.size(); i++){
            PorterStemmer stemmer = new PorterStemmer();
            stemmer.add(wordsIntermediate.get(i).trim().toCharArray(), wordsIntermediate.get(i).length());
            stemmer.stem();
            String stemmed = stemmer.toString();
            words[i] = stemmed;
        }
        ArrayList<String> ans = new ArrayList<>();
        for(int j = 0; j < words.length; j++){
            byte[] urlWithTFIDFByte = kvsClient.get("pt-TFIDF", words[j], "value");
            if (urlWithTFIDFByte == null) {
                urlWithTFIDFByte = kvsClient.get("pt-TFIDF", words[j], "acc");
            }
            String urlWithTFIDF = null;
            if (urlWithTFIDFByte != null){
                urlWithTFIDF = new String(urlWithTFIDFByte, StandardCharsets.UTF_8);

                byte[] urlWithTFIDFByteSecond;
                String urlWithTFIDFSecond = null;
                if(words.length>(j+1)) {
                    urlWithTFIDFByteSecond = kvsClient.get("pt-TFIDF", words[j+1], "value");
                    if (urlWithTFIDFByteSecond == null) {
                        urlWithTFIDFByteSecond = kvsClient.get("pt-TFIDF", words[j+1], "acc");
                    }
                    if(urlWithTFIDFByteSecond!=null){
                        urlWithTFIDFSecond = new String(urlWithTFIDFByteSecond, StandardCharsets.UTF_8);
                    }
                }

                if (urlWithTFIDF != null) {
                    String[] urlsWithTFIDF = urlWithTFIDF.split(" URLSEPARATOR ");
                    Arrays.sort(urlsWithTFIDF);
                    for(int i = 0; i<urlsWithTFIDF.length; i++) {
                        String[] urls = urlsWithTFIDF[i].split(": occurrences ");
                        String[] tfidf = urlsWithTFIDF[i].split(" -TFIDF- ");
                        String url = urls[0].trim();
                        Double TFIDFPG = Double.parseDouble(tfidf[1]);
                        ans.add(tfidf[0] + " -TFIDFPG- " + TFIDFPG);
                        if (urlWithTFIDFSecond != null) {
                            String[] urlsWithTFIDFSecond = urlWithTFIDFSecond.toString().split(" URLSEPARATOR ");
                            Arrays.sort(urlsWithTFIDFSecond);
                            for (int z = 0; z < urlsWithTFIDFSecond.length; z++) {
                                String[] urlsSecond = urlsWithTFIDFSecond[z].split(": occurrences ");
                                String urlSecond = urlsSecond[0].trim();
                                int comparisonResult = url.compareTo(urlSecond);
                                if(comparisonResult < 0){
                                    break;
                                } else if (comparisonResult > 0){
                                    continue;
                                } else {
                                    String[] analyzeData = urls[1].trim().split(" ");//0 is occurrences, 2 is total words, 4 and above are positions
                                    String[] analyzeDataSecond = urlsSecond[1].trim().split(" ");//0 is occurrences, 2 is total words, 4 and above are positions
                                    int min = Integer.MAX_VALUE;
                                    int k = 4, t = 4;
                                    while (k < analyzeData.length && t < analyzeDataSecond.length) {
                                        try {
                                            int value = Integer.parseInt(analyzeData[k]);
                                            int valueSecond = Integer.parseInt(analyzeDataSecond[t]);

                                            int currentDistance = Math.abs(valueSecond - value);
                                            min = Math.min(min, currentDistance);
                                            if (value < valueSecond) {
                                                k++;
                                            } else {
                                                t++;
                                            }
                                        } catch (NumberFormatException e) {
                                            break;
                                        }
                                    }
                                    if (min < 2) {
                                        ans.add(tfidf[0] + " -TFIDFPG- " + 6);
                                    } else if (min < 5) {
                                        ans.add(tfidf[0] + " -TFIDFPG- " + 3);
                                    }
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }
//        System.out.println(ans); //supposed to be http://ddddd -TFIDFPG- 0.0001, http://ddddd -TFIDFPG- 6

        HashMap<String, Double> urlAndRank = new HashMap<>();
        for(String s: ans){
            String[] byTFIDFPG = s.split(" -TFIDFPG- ");
            String[] url = byTFIDFPG[0].split(": occurrences ");
            urlAndRank.merge(Hasher.hash(url[0].trim()) + "-url-" + url[0].trim(), Double.parseDouble(byTFIDFPG[1]), Double::sum);
        }
        return urlAndRank;
    }
}
