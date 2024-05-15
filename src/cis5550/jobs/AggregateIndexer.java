package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlameRDD;

public class AggregateIndexer {
    public static void run(FlameContext flameContext, String[] args) throws Exception {
        FlameRDD tableOne = flameContext.fromTable("pt-index", row -> {
            String url = row.get("url");
            String page = row.get("value");
            String hashUrl = row.key();
            return url + " ZKLURLANDPAGESEPARATOR " + page + " ZKLURLANDPAGESEPARATOR " + hashUrl;
        });
    }
}
