package cis5550.jobs;

import cis5550.external.PorterStemmer;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlameRDD;
import cis5550.tools.Constants;
import cis5550.tools.Hasher;
import cis5550.tools.Logger;
import cis5550.tools.ReservedNames;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ContentIndexer {
    private static final Map<String, String> stemCache = new HashMap<>();
    private static final double MAX_NUM = 999999;
    private static final String DELIMITER = " ZKLURLANDPAGESEPARATOR ";
    private static final Logger logger = Logger.getLogger(ContentIndexer.class);

    public static void run(FlameRDD flameRDD) throws Exception {

        FlamePairRDD out = flameRDD.mapToPair(line -> {
            if (line == null || line.isEmpty()) {
                return null;
            }
            String[] parts = line.split(DELIMITER, 2);
            return new FlamePair(parts[0], parts[1]);
        }).flatMapToPair(pair -> {
            if (pair == null) {
                return new ArrayList<>();
            }

            String hashKey = pair._1();
            String info = pair._2().trim();
            String[] parts = info.split(DELIMITER, 4);
            if (parts.length == 1) {
                return new ArrayList<>();
            }
            String url = parts[0];
            String title = parts[1] != null && !parts[1].equals("EMPTYTITLE") ? parts[1] : "";
            String description = parts[2] != null && !parts[2].equals("EMPTYDESCRIPTION") ? parts[2] : "";
            String pageContent = "";
            try {
                pageContent = parts[3] != null && !parts[3].isEmpty() ? parts[3] : "";
            } catch (Exception e) {
                logger.info("Empty page content");
            }

            if (title.isEmpty() && description.isEmpty() && pageContent.isEmpty()) {
                return new ArrayList<>();
            }

            Set<String> titleWords = new HashSet<>(Arrays.asList(title.split("\\s+")));
            Set<String> descWords = new HashSet<>(Arrays.asList(description.split("\\s+")));

            String[] words = (title + " " + description + " " + pageContent).split("\\s+");
            int totalWords = words.length;

            Map<String, List<Integer>> wordPositions = new ConcurrentHashMap<>();
            for (int i = 0; i < words.length; i++) {
                String curWord = decodeIfNeeded(words[i]);
                if (curWord != null && !curWord.isEmpty() && !Constants.STOP_WORDS.contains(curWord)
                        && !ReservedNames.RESERVED_NAMES.contains(curWord) && isValidWord(curWord)) {
                    wordPositions.computeIfAbsent(curWord, k -> new ArrayList<>()).add(i + 1);
                    String stemmed = getStemmedWord(curWord);
                    if (!stemmed.equals(curWord) && !Constants.STOP_WORDS.contains(stemmed)
                            && !ReservedNames.RESERVED_NAMES.contains(stemmed) && isValidWord(stemmed)) {
                        wordPositions.computeIfAbsent(stemmed, k -> new ArrayList<>()).add(i + 1);
                    }
                }
            }

            List<FlamePair> pairs = new ArrayList<>();
            wordPositions.forEach((word, positions) -> {
                int occurrences = positions.size();
                if (titleWords.contains(word)) {
                    logger.info("cur url: " + url);
                    logger.info("word found in title: " + word);
                    occurrences += (occurrences * 20);
                }
                if (descWords.contains(word)) {
                    logger.info("cur url: " + url);
                    logger.info("word found in description: " + word);
                    occurrences += (occurrences * 10);
                }
                StringBuilder positionsStringBuilder = new StringBuilder();
                positionsStringBuilder.append(" occurrences ").append(occurrences);
                positionsStringBuilder.append(" totalWords ").append(totalWords);
                positionsStringBuilder.append(" positions ");
                for (int pos : positions) {
                    positionsStringBuilder.append(pos).append(" ");
                }
                String positionsString = positionsStringBuilder.toString().trim();
                pairs.add(new FlamePair(word, url + ": " + positionsString));
            });
            return pairs;
        }).foldByKey("", (acc, urlWithMetrics) -> acc.isEmpty() ? urlWithMetrics : acc + " URLSEPARATOR " + urlWithMetrics);

//        invertedIndex.destroy();
//
//        FlamePairRDD sortedWordAndUrl = foldedInvertedIndex.flatMapToPair(pair -> {
//            String word = pair._1();
//            String metricString = pair._2();
//            String[] urlWithMetricsList = metricString.split(",");
//
//            Map<String, AbstractMap.SimpleEntry<String, Integer>> urlWithMetrics = new ConcurrentHashMap<>();
//            for (String element : urlWithMetricsList) {
//                String curUrl = element.substring(0, element.lastIndexOf(":"));
//                String metricsString = element.substring(element.lastIndexOf(":") + 1).trim();
//                int occurrences = extractOccurrences(element);
//                urlWithMetrics.computeIfAbsent(curUrl, k -> new AbstractMap.SimpleEntry<>(metricsString, occurrences));
////                        String[] positions = element.substring(element.lastIndexOf("positions:") + 1).trim().split("\\s+");
////                        for (String pos : positions) {
////                            urlWithPositions.computeIfAbsent(curUrl, k -> new ArrayList<>()).add(pos);
////                        }
//            }
//
//            Map<String, AbstractMap.SimpleEntry<String, Integer>> sortedMap = sortMapByOccurrences(urlWithMetrics);
//            StringBuilder sortedUrlWithMetrics = new StringBuilder();
//            for (Map.Entry<String, AbstractMap.SimpleEntry<String, Integer>> entry : sortedMap.entrySet()) {
//                sortedUrlWithMetrics.append(entry.getKey()).append(": ");
//                sortedUrlWithMetrics.append(entry.getValue().getKey());
//                sortedUrlWithMetrics.append(", ");
//            }
//            if (!sortedUrlWithMetrics.isEmpty()) {
//                sortedUrlWithMetrics.delete(sortedUrlWithMetrics.length() - 2, sortedUrlWithMetrics.length());
//            }
//            return Arrays.asList(new FlamePair(word, sortedUrlWithMetrics.toString()));
//        });
//
//        foldedInvertedIndex.destroy();
//
//        FlamePairRDD foldedWordAndUrl = sortedWordAndUrl.foldByKey("", (acc, urlWithPositions) -> acc.isEmpty() ? urlWithPositions : acc + ", " + urlWithPositions);
//
//        sortedWordAndUrl.destroy();

        out.saveAsTable("pt-index");
    }

    public static String getStemmedWord(String word) {
        if (stemCache.containsKey(word)) {
            return stemCache.get(word);
        }

        PorterStemmer stemmer = new PorterStemmer();
        stemmer.add(word.toCharArray(), word.length());
        stemmer.stem();
        String stemmedWord = stemmer.toString();

        stemCache.put(word, stemmedWord);
        return stemmedWord;
    }

//    private static Map<String, AbstractMap.SimpleEntry<String, Integer>> sortMapByOccurrences(Map<String, AbstractMap.SimpleEntry<String, Integer>> originalMap) {
//        Comparator<String> sizeComparator = (key1, key2) -> {
//            int size1 = originalMap.get(key1).getValue();
//            int size2 = originalMap.get(key2).getValue();
//            if (size1 == size2) {
//                return key1.compareTo(key2);
//            }
//            return Integer.compare(size2, size1);
//        };
//
//        Map<String, AbstractMap.SimpleEntry<String, Integer>> sortedMap = new TreeMap<>(sizeComparator);
//        sortedMap.putAll(originalMap);
//        return sortedMap;
//    }

//    public static int extractOccurrences(String input) {
//        Pattern pattern = Pattern.compile("occurrences\\s+(\\d+)");
//        Matcher matcher = pattern.matcher(input);
//
//        if (matcher.find()) {
//            return Integer.parseInt(matcher.group(1));
//        } else {
//            throw new IllegalArgumentException("The input string does not contain 'occurrences'");
//        }
//    }

    private static String decodeIfNeeded(String word) throws Exception {
        try {
            String decodedWord = URLDecoder.decode(word, StandardCharsets.UTF_8);
            return !decodedWord.equals(word) ? decodedWord : word;
        } catch (IllegalArgumentException e) {
            return word;
        }
    }

    private static boolean isValidWord(String str) {
        Pattern nonAlNum = Pattern.compile("[^a-zA-Z0-9]");
        Matcher nonAlNumMatcher = nonAlNum.matcher(str);
        if (nonAlNumMatcher.matches()) {
            return false;
        }
        Pattern al_num_mix = Pattern.compile(".*\\d.*\\p{Alpha}.*|.*\\p{Alpha}.*\\d.*");
        Matcher alnum_mix_matcher = al_num_mix.matcher(str);
        if (alnum_mix_matcher.matches()) {
            return false;
        }

        try {
            double d = Double.parseDouble(str);
            if (d > MAX_NUM || str.length() > 6) {
                return false;
            }
        } catch (NumberFormatException nfe) {
            return str.length() > 1 && str.length() <= 31;
        }
        return str.length() > 1;

    }
}