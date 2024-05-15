package cis5550.jobs;

import cis5550.external.PorterStemmer;
import cis5550.flame.*;
import cis5550.tools.Hasher;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

public class TFIDF {
    public static void run(FlameContext context, String[] args) throws Exception {

        int count = context.getKVS().count("pt-pageranks"); //total number of urls
        FlameRDD ft = context.fromTable("pt-index", r -> r.key() + "," + r.get("value"));

        FlamePairRDD out = ft.flatMapToPair(s -> {
            int firstComma = s.indexOf(',');
            FlamePair pair = new FlamePair(""+s.substring(0, firstComma), s.substring(firstComma+1));
            return Arrays.asList(pair);
        });
        ft.destroy();

        FlamePairRDD invertedIndex = out.flatMapToPair(pair -> {
            ArrayList<FlamePair> ans = new ArrayList<>();
            String[] allURLS = pair._2().split(" URLSEPARATOR ");
            for(String url: allURLS){
                String[] totalAnalyzeData = url.split(": occurrences ");
                String[] analyzeData = totalAnalyzeData[1].split(" ");//0 is occurrences, 2 is total words, 4 and above are positions
                Double tf = Double.parseDouble(analyzeData[0]) / Double.parseDouble(analyzeData[2]);
                ans.add(new FlamePair(pair._1(), url + " -TF- " + tf));
            }
            return ans;
        }).foldByKey("", (acc,url) -> acc.isEmpty() ? url : acc + " URLSEPARATOR " + url);
//        invertedIndex.saveAsTable("pt-invertedIndex");
        out.destroy();

        FlamePairRDD dfPairs = invertedIndex.foldByKey("", (acc, url) -> {
            // Assuming url is a String that contains one or more instances separated by " -TF- "
            String keyword = " -TF- ";
            int ct = 0;

            for (int index = url.indexOf(keyword);
                 index >= 0;
                 index = url.indexOf(keyword, index + keyword.length())) {
                ct++;
            }
            return String.valueOf(ct);
        });
//        System.out.println("count: " + count );

        FlamePairRDD dfCal = dfPairs.foldByKey("", (acc, url) -> {
//            System.out.println("URL: " + url);
            return String.valueOf(Math.log(count/Double.parseDouble(url)));
        });
//        dfCal.saveAsTable("pt-dfCal");
//        dfPairs.destroy();
        // Assuming url is a String that contains one or more instances separated by " -TF- "
        FlamePairRDD joinTable = invertedIndex.join(dfCal);
//        dfCal.destroy();
//        invertedIndex.destroy();


        FlamePairRDD finalTFIDF = joinTable.flatMapToPair(pair -> {
            ArrayList<FlamePair> ans = new ArrayList<FlamePair>();
            String word = pair._1();
            int lastComma = pair._2().lastIndexOf(" URLSEPARATOR ");


            try {
                String[] transition1 = pair._2().substring(0, lastComma).split(" URLSEPARATOR ");
                Double idf = Double.parseDouble(pair._2().substring(lastComma+" URLSEPARATOR ".length()).trim()); //idf is at the last comma
                for (int i = 0; i< transition1.length; i++){
                    String[] transition2 = transition1[i].split(" -TF- ");
                    Double tf = Double.parseDouble(transition2[1].trim());
                    ans.add(new FlamePair(word,  transition2[0].trim() + " -TFIDF- " + tf*idf));
                }
            } catch (Exception e) {
                System.out.println("cur word is: " + word);
                System.out.println("cur string: " + pair._2());
                System.out.println("Exception in final TFIDF: " + e.getMessage());
            }

            return ans;
        }).foldByKey("", (acc,url) -> acc.isEmpty() ? url : acc + " URLSEPARATOR " + url);
        joinTable.destroy();

        finalTFIDF.saveAsTable("pt-TFIDF");

    }
}
