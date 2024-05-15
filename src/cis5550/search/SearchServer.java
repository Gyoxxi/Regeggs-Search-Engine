package cis5550.search;

import cis5550.tools.autocomplete.Autocomplete;

import java.nio.file.*;
import java.util.Set;
import java.util.stream.Collectors;

import static cis5550.search.ResultFactory.*;
import static cis5550.webserver.Server.*;

public class SearchServer {
    static Autocomplete ac = new Autocomplete(5);
    public static void main(String[] args) throws Exception {
        port(80);
        staticFiles.location("src/resources/webapp");
        get("/", (req, res) -> {
            try {
                Path p = Paths.get("src/resources/webapp/index.html");
                String content = new String(Files.readAllBytes(p));
                res.type("text/html");
                return content;
            } catch (Exception e) {
                res.status(500, "Internal Server Error");
                return "Internal Server Error";
            }
        });

        get("/search", (req, res) -> {
            String queryTerms = req.queryParams("q");
            if (queryTerms == null || queryTerms.trim().isEmpty()) {
                res.status(404, "Not Found");
                return "Not Found";
            }
            int offset;
            String pageNumber = req.headers("x-page-number");
            offset = Integer.parseInt(pageNumber);
            try {
                SearchResults results = createResults(queryTerms, offset);
                res.type("application/json");
                return results.toJson();
            } catch (Exception e) {
                res.status(500, "Internal Server Error");
                return "Internal Server Error";
            }
        });

        get("/preview", (req, res) -> {
            String rowKey = req.queryParams("r");
            if (rowKey == null || rowKey.trim().isEmpty()) {
                res.status(404, "Not Found");
                return "Not Found";
            }
            try {
                PagePreview preview = createPagePreview(rowKey);
                res.type("text/html");
                return preview.toJson();
            } catch (Exception e) {
                res.status(500, "Internal Server Error");
                return "Internal Server Error";
            }
        });

        get("/autocomplete", (req, res) -> {
            String prefix = req.queryParams("p");
            if (prefix == null || prefix.trim().isEmpty()) {
                res.status(400, "Query Parameter Missing");
                return "Query Parameter Missing";
            }

            try {
                Set<String> suggestions = ac.getSuggestions(prefix.trim());
                res.type("application/json");
                return suggestions.stream().collect(Collectors.joining("\",\"", "[\"", "\"]"));
            } catch (Exception e) {
                res.status(500, "Internal Server Error");
                return "Internal Server Error";
            }
        });

    }

}