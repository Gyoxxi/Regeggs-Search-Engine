package cis5550.flame;

import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;
import cis5550.tools.Serializer;
import cis5550.webserver.Request;
import cis5550.webserver.Response;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;

import static cis5550.webserver.Server.*;

class Worker extends cis5550.generic.Worker {

    private static File myJAR;
    private static final String LINK_DELIMITER = " URLSEPARATOR ";

    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.err.println("Syntax: Worker <port> <coordinatorIP:port>");
            System.exit(1);
        }

        int port = Integer.parseInt(args[0]);
        String server = args[1];
        startPingThread(port, "" + port, server);
        myJAR = new File("__worker" + port + "-current.jar");

        port(port);

        post("/useJAR", (request, response) -> {
            FileOutputStream fos = new FileOutputStream(myJAR);
            fos.write(request.bodyAsBytes());
            fos.close();
            return "OK";
        });

        RowOperation mapToPairRowOperation = (request, kvs, row, lambda, outputTable) -> {
            String value = row.get("value");
            FlamePair flamePair = ((FlameRDD.StringToPair) lambda).op(value);
            if (flamePair == null) {
                return;
            }
            kvs.put(outputTable, flamePair._1(), row.key(), flamePair._2());
        };

        RowOperation flatMapRowOperation = (request, kvs, row, lambda, outputTable) -> {
            String value = row.get("value");
            Iterable<String> results = ((FlameRDD.StringToIterable) lambda).op(value);
            if (results == null) {
                return;
            }
            int i = 0;
            for (String result : results) {
                kvs.put(outputTable, Hasher.hash(row.key() + i), "value", result);
                i++;
            }
        };

        RowOperation flatMapToPairRowOperation = (request, kvs, row, lambda, outputTable) -> {
            String value = row.get("value");
            Iterable<FlamePair> results = ((FlameRDD.StringToPairIterable) lambda).op(value);
            if (results == null) {
                return;
            }
            int i = 0;
            for (FlamePair result : results) {
                kvs.put(outputTable, result._1(), Hasher.hash(row.key() + i), result._2());
                i++;
            }
        };

        RowOperation distinctRowOperation = (request, kvs, row, lambda, outputTable) -> {
            String value = row.get("value");
            kvs.put(outputTable, value, "value", value);
        };

        RowOperation pairFoldByKeyRowOperation = (request, kvs, row, lambda, outputTable) -> {
            String accumulator = request.queryParams("zeroElement");
            for (String column : row.columns()) {
                String value = row.get(column);
                accumulator = ((FlamePairRDD.TwoStringsToString) lambda).op(accumulator, value);
            }
            kvs.put(outputTable, row.key(), "value", accumulator);
        };

        RowOperation pairFlatMapRowOperation = (request, kvs, row, lambda, outputTable) -> {
            for (String column : row.columns()) {
                String value = row.get(column);
                FlamePair flamePair = new FlamePair(row.key(), value);
                Iterable<String> results = ((FlamePairRDD.PairToStringIterable) lambda).op(flamePair);
                if (results == null) {
                    return;
                }
                int i = 0;
                for (String result : results) {
                    kvs.put(outputTable, Hasher.hash(row.key() + i), "value", result);
                    i++;
                }
            }
        };

        RowOperation pairFlatMapToPairRowOperation = (request, kvs, row, lambda, outputTable) -> {
            for (String column : row.columns()) {
                String value = row.get(column);
                FlamePair flamePair = new FlamePair(row.key(), value);
                Iterable<FlamePair> results = ((FlamePairRDD.PairToPairIterable) lambda).op(flamePair);
                if (results == null) {
                    return;
                }
                int i = 0;
                for (FlamePair result : results) {
                    kvs.put(outputTable, result._1(), Hasher.hash(row.key() + i), result._2());
                    i++;
                }
            }
        };

        RowOperation pairJoinRowOperation = (request, kvs, row, lambda, outputTable) -> {
            String otherTable = request.queryParams("otherTable");
            Row otherRow = kvs.getRow(otherTable, row.key());
            if (otherRow == null) {
                return;
            }
            for (String column : row.columns()) {
                String value = row.get(column);
                for (String otherColumn : otherRow.columns()) {
                    String otherValue = otherRow.get(otherColumn);
                    String outputColumnName = Hasher.hash(column) + "_" + Hasher.hash(otherColumn);
                    String outputValue = value + LINK_DELIMITER + otherValue;
                    kvs.put(outputTable, row.key(), outputColumnName, outputValue);
                }
            }
        };

        RowOperation fromTableRowOperation = (request, kvs, row, lambda, outputTable) -> {
            String result = ((FlameContext.RowToString) lambda).op(row);
            if (result == null) {
                return;
            }
            kvs.put(outputTable, row.key(), "value", result);
        };

        post("/rdd/mapToPair", (request, response) -> handleRDDOperation(request, mapToPairRowOperation));
        post("/rdd/flatMap", (request, response) -> handleRDDOperation(request, flatMapRowOperation));
        post("/rdd/flatMapToPair", (request, response) -> handleRDDOperation(request, flatMapToPairRowOperation));
        post("/rdd/distinct", (request, response) -> handleRDDOperation(request, distinctRowOperation));
        post("/rdd/fold", Worker::handleFoldOperation);

        post("/pairRdd/foldByKey", (request, response) -> handleRDDOperation(request, pairFoldByKeyRowOperation));
        post("/pairRdd/flatMap", (request, response) -> handleRDDOperation(request, pairFlatMapRowOperation));
        post("/pairRdd/flatMapToPair", (request, response) -> handleRDDOperation(request, pairFlatMapToPairRowOperation));
        post("/pairRdd/join", (request, response) -> handleRDDOperation(request, pairJoinRowOperation));

        post("/context/fromTable", (request, response) -> handleRDDOperation(request, fromTableRowOperation));
    }

    private static String handleRDDOperation(Request request, RowOperation rowOperation) throws Exception {
        String inputTable = request.queryParams("inputTableName");
        String outputTable = request.queryParams("outputTableName");
        String kvsCoordinator = request.queryParams("kvsCoordinator");
        String fromKey = request.queryParams("fromKey");
        String toKeyExclusive = request.queryParams("toKeyExclusive");

        byte[] requestBody = request.bodyAsBytes();
        Object lambda = (requestBody != null && requestBody.length > 0) ? Serializer.byteArrayToObject(requestBody, myJAR) : null;

        KVSClient kvsClient = Coordinator.kvs = new KVSClient(kvsCoordinator);
        Iterator<Row> rowIter = kvsClient.scan(inputTable, fromKey, toKeyExclusive);
        while (rowIter.hasNext()) {
            Row row = rowIter.next();
            rowOperation.apply(request, kvsClient, row, lambda, outputTable);
        }

        return "OK";
    }

    private static String handleFoldOperation(Request request, Response response) throws IOException {
        String inputTable = request.queryParams("inputTableName");
        String kvsCoordinator = request.queryParams("kvsCoordinator");
        String fromKey = request.queryParams("fromKey");
        String toKeyExclusive = request.queryParams("toKeyExclusive");
        String accumulator = request.queryParams("zeroElement");

        Object lambda = Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);

        KVSClient kvsClient = Coordinator.kvs = new KVSClient(kvsCoordinator);
        Iterator<Row> rowIter = kvsClient.scan(inputTable, fromKey, toKeyExclusive);
        while (rowIter.hasNext()) {
            Row row = rowIter.next();
            String value = row.get("value");
            accumulator = ((FlamePairRDD.TwoStringsToString) lambda).op(accumulator, value);
        }

        return accumulator;
    }

    interface RowOperation {
        void apply(Request request, KVSClient kvsClient, Row row, Object lambda, String outputTable) throws Exception;
    }
}
