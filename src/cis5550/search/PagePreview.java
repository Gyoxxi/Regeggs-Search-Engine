package cis5550.search;

public class PagePreview {
    private final String pageContent;

    public PagePreview(String pageContent) {
        this.pageContent = pageContent;
    }

    public String toJson() {
        return String.format("{\"page\": \"%s\"}", escapeJson(pageContent));
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
}
