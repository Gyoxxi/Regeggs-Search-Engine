package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlameRDD;
import cis5550.flame.FlameRDDImpl;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;

import java.io.BufferedReader;
import java.io.FileReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class WordsLoader {
    private static final String filePath = "src/resources/words.txt";
    public static void run(FlameContext context, String[] args) throws Exception {
        readWordsFile(context);
    }

    private static void readWordsFile(FlameContext context) throws Exception {
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.trim().isEmpty()) {
                    String rowKey = Hasher.hash(line);
                    Row row = new Row(rowKey);
                    row.put("value", line);
                    context.getKVS().putRow("pt-words", row);
                }
            }
        } catch (Exception e) {
            System.err.println("Failed to read words file: " + e.getMessage());
            throw e;
        }
    }
}
