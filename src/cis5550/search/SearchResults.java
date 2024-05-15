package cis5550.search;


public class SearchResults {
    private final Iterable<Result> results;

    public SearchResults(Iterable<Result> results) {
        this.results = results;
    }

    public String toJson() {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"results\":[");
        boolean first = true;
        for (Result result : results) {
            if (!first) {
                sb.append(",");
            }
            sb.append(result.toJson());
            first = false;
        }
        sb.append("]}");
        return sb.toString();
    }


    public static class Result implements Comparable<Result>{
        private String rowKey;
        private double score;
        private String url;
        private String title;
        private String snippet;

        private String hostname;

        public Result(String url, String hostname, String title, String snippet, double score, String rowKey) {
            this.url = url;
            this.title = title;
            this.snippet = snippet;
            this.hostname = hostname;
            this.score = score;
            this.rowKey = rowKey;
        }

        public String toJson() {
            return String.format("{\"hostname\":\"%s\",\"url\":\"%s\",\"title\":\"%s\",\"snippet\":\"%s\", \"rowKey\":\"%s\"}",
                    escapeJson(hostname),escapeJson(url), escapeJson(title), escapeJson(snippet), escapeJson(rowKey));
        }

        @Override
        public int compareTo(Result other) {
            // Sort in descending order by score
            if (other.score == this.score) {
                return String.CASE_INSENSITIVE_ORDER.compare(other.url, this.url);
            }
            return Double.compare(other.score, this.score);
        }

        private String escapeJson(String s) {
            return s.replace("\\", "\\\\")
                    .replace("\"", "\\\"")
                    .replace("\b", "\\b")
                    .replace("\f", "\\f")
                    .replace("\n", "\\n")
                    .replace("\r", "\\r")
                    .replace("\t", "\\t");
        }

        // Getters and setters

        public double getScore() {
            return score;
        }

        public void setScore(double score) {
            this.score = score;
        }


        public String getRowKey() {
            return rowKey;
        }

        public void setRowKey(String rowKey) {
            this.rowKey = rowKey;
        }
        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public String getTitle() {
            return title;
        }

        public void setTitle(String title) {
            this.title = title;
        }

        public String getSnippet() {
            return snippet;
        }

        public void setSnippet(String snippet) {
            this.snippet = snippet;
        }

        public String getHostname() {
            return hostname;
        }

        public void setHostname(String hostname) {
            this.hostname = hostname;
        }
    }
}

