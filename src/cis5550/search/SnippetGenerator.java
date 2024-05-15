package cis5550.search;

import opennlp.tools.sentdetect.*;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.*;
import java.util.regex.Pattern;

public class SnippetGenerator {
    private SentenceDetectorME sentenceDetector;

    public boolean isTermFound() {
        return isTermFound;
    }

    private boolean isTermFound = false;

    public SnippetGenerator() {
        try {
            InputStream modelIn = new FileInputStream("src/resources/models/en-sent.bin");
            SentenceModel model = new SentenceModel(modelIn);
            this.sentenceDetector = new SentenceDetectorME(model);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String generateSnippet(String htmlContent, Set<String> queryTerms, int snippetLength) {
        htmlContent = htmlContent.replaceAll("<[^>]*>", "").replaceAll("\\s+", " ");
        String[] sentences = sentenceDetector.sentDetect(htmlContent);
        String bestSentence = "";
        int maxTermCount = 0;
        int minDistance = Integer.MAX_VALUE; // Initialize to a very high value

        for (String sentence : sentences) {
            Set<String> wordsInSentence = new HashSet<>(Arrays.asList(sentence.toLowerCase().split("\\W+")));
            Set<String> commonTerms = new HashSet<>(wordsInSentence);
            commonTerms.retainAll(queryTerms);

            int termCount = commonTerms.size();
            if (termCount > maxTermCount || (termCount == maxTermCount && getDistance(sentence, queryTerms) < minDistance)) {
                maxTermCount = termCount;
                minDistance = getDistance(sentence, queryTerms);
                bestSentence = sentence;
            }
        }

        if (maxTermCount == 0) {
            isTermFound = false;
            return firstSentenceSnippet(sentences, snippetLength);
        }

        isTermFound = true;
        String snippet = getSnippet(snippetLength, bestSentence, queryTerms);
        return highlightTerms(queryTerms, snippet);
    }

    private String firstSentenceSnippet(String[] sentences, int snippetLength) {
        if (sentences == null || sentences.length == 0) {
            return "Snippet not available.";
        }

        String sentencesString = String.join(" ", sentences);

        String snippet = sentencesString.substring(0, snippetLength);

        // Append ellipsis if content is longer than the snippet length
        if (sentencesString.length() > snippetLength) {
            snippet += " ...";
        } else if (!snippet.endsWith(".")) {
            snippet += ".";
        }

        return snippet;
    }

    private static String highlightTerms(Set<String> queryTerms, String snippet) {
        for (String term : queryTerms) {
            snippet = snippet.replaceAll("(?i)" + Pattern.quote(term), "<b>$0</b>");
        }

        if (!startsWithCapital(snippet)) {
            snippet = "... " + snippet;
        }

        snippet += " ...";
        return snippet;
    }

    private static boolean startsWithCapital(String input) {
        // Check if the string is not empty
        if (input == null || input.isEmpty()) {
            return false;
        }

        // Get the first character of the string
        char firstChar = input.charAt(0);

        // Check if the first character is an uppercase letter
        return Character.isUpperCase(firstChar);
    }

    private static String getSnippet(int snippetLength, String bestSentence, Set<String> queryTerms) {
        // Find the index of the first query term
        int firstTermIndex = -1;
        for (String term : queryTerms) {
            int index = bestSentence.toLowerCase().indexOf(term.toLowerCase());
            if (index != -1 && (firstTermIndex == -1 || index < firstTermIndex)) {
                firstTermIndex = index;
            }
        }

        // Calculate start and end indices to center the snippet around the first query term found
        return extractSubstring(snippetLength, firstTermIndex, bestSentence);
    }

    private static String extractSubstring(int snippetLength, int termIndex, String sentence) {
        int start = Math.max(0, termIndex - snippetLength / 2);
        int end = Math.min(sentence.length(), start + snippetLength);
        if (start > 0) {
            start = sentence.lastIndexOf(' ', start) + 1; // Adjust to start at a complete word
        }
        if (end < sentence.length()) {
            end = sentence.indexOf(' ', end); // Adjust to end at a complete word
            if (end == -1) { // If no space is found, take till the end of the sentence
                end = sentence.length();
            }
        }
        return sentence.substring(start, end);
    }

    private static int getDistance(String sentence, Set<String> queryTerms) {
        List<Integer> indices = new ArrayList<>();
        for (String term : queryTerms) {
            int index = sentence.toLowerCase().indexOf(term.toLowerCase());
            if (index != -1) {
                indices.add(index);
            }
        }
        Collections.sort(indices);
        int totalDistance = 0;
        for (int i = 1; i < indices.size(); i++) {
            totalDistance += indices.get(i) - indices.get(i - 1);
        }
        return totalDistance;
    }
}
