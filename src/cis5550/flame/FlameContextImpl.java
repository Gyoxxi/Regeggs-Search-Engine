package cis5550.flame;

import cis5550.kvs.KVSClient;
import cis5550.tools.HTTP;
import cis5550.tools.Hasher;
import cis5550.tools.Partitioner;
import cis5550.tools.Serializer;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Vector;

public class FlameContextImpl implements FlameContext, Serializable {

    private final File myJAR;
    public String output;

    public FlameContextImpl(String jarName) {
        this.output = "";
        String jarAbsolutePath = FileSystems.getDefault().getPath(jarName).normalize().toAbsolutePath().toString();
        this.myJAR = new File(jarAbsolutePath);
    }

    @Override
    public KVSClient getKVS() {
        return Coordinator.kvs;
    }

    @Override
    public void output(String s) {
        this.output += s;
    }

    @Override
    public FlameRDD parallelize(List<String> list) throws Exception {
        String tableName = this.createTableName();
        KVSClient kvsClient = getKVS();

        int i = 1;
        for (String value : list) {
            kvsClient.put(tableName, Hasher.hash(String.valueOf(i)), "value", value);
            i++;
        }

        return new FlameRDDImpl(this, tableName);
    }

    @Override
    public FlameRDD fromTable(String tableName, RowToString lambda) throws Exception {
        String operationName = "/context/fromTable";
        byte[] lambdaInBytes = Serializer.objectToByteArray(lambda);
        String outputTableName = this.invokeOperation(operationName, lambdaInBytes, tableName);
        return new FlameRDDImpl(this, outputTableName);
    }

    @Override
    public void setConcurrencyLevel(int keyRangesPerWorker) {

    }

    public String invokeOperation(
            String operationName, byte[] lambda, String inputTableName, String... args) throws Exception {
        Partitioner partitioner = new Partitioner();

        KVSClient kvsClient = getKVS();
        int numWorkers = kvsClient.numWorkers();
        for (int i = 0; i < numWorkers; i++) {
            String workerAddress = kvsClient.getWorkerAddress(i);
            String workerId = kvsClient.getWorkerID(i);
            if (i == numWorkers - 1) {
                partitioner.addKVSWorker(workerAddress, workerId, null);
                String lowestWorkerId = kvsClient.getWorkerID(0);
                partitioner.addKVSWorker(workerAddress, null, lowestWorkerId);
                continue;
            }
            String nextWorkerId = kvsClient.getWorkerID(i + 1);
            partitioner.addKVSWorker(workerAddress, workerId, nextWorkerId);
        }

        List<String> workers = Coordinator.getWorkers();
        for (String worker : workers) {
            partitioner.addFlameWorker(worker);
        }
        Vector<Partitioner.Partition> partitions = partitioner.assignPartitions();

        String outputTableName = this.createTableName();
        Thread[] threads = new Thread[partitions.size()];
        HTTP.Response[] responses = new HTTP.Response[partitions.size()];
        for (int i = 0; i < partitions.size(); i++) {
            Partitioner.Partition partition = partitions.get(i);
            StringBuilder url = new StringBuilder();
            url.append("http://").append(partition.assignedFlameWorker).append(operationName);
            url.append("?inputTableName=").append(URLEncoder.encode(inputTableName, StandardCharsets.UTF_8));
            url.append("&outputTableName=").append(URLEncoder.encode(outputTableName, StandardCharsets.UTF_8));
            url.append("&kvsCoordinator=").append(URLEncoder.encode(kvsClient.getCoordinator(), StandardCharsets.UTF_8));
            if (partition.fromKey != null) {
                url.append("&fromKey=").append(URLEncoder.encode(partition.fromKey, StandardCharsets.UTF_8));
            }
            if (partition.toKeyExclusive != null) {
                url.append("&toKeyExclusive=").append(URLEncoder.encode(partition.toKeyExclusive, StandardCharsets.UTF_8));
            }
            for (String arg : args) {
                // Assuming 'arg' is already in the format "key=value"
                String[] keyValue = arg.split("=", 2);
                String encodedKey = URLEncoder.encode(keyValue[0], StandardCharsets.UTF_8);
                String encodedValue = URLEncoder.encode(keyValue[1], StandardCharsets.UTF_8);
                url.append("&").append(encodedKey).append("=").append(encodedValue);
            }
            int finalI = i;
            Thread thread = new Thread(() -> {
                try {
                    HTTP.Response response = HTTP.doRequest("POST", url.toString(), lambda);
                    responses[finalI] = response;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            threads[i] = thread;
            thread.start();
        }

        boolean isFoldOperation = Arrays.asList(args).contains("isFoldOperation=true");
        if (isFoldOperation) {
            Optional<String> optional = Arrays.stream(args).filter(x -> x.contains("zeroElement")).findFirst();
            if (optional.isEmpty()) {
                throw new Exception("Missing zeroElement");
            }
            String zeroElementQueryParam = optional.get();
            String accumulator = zeroElementQueryParam.split("=")[1];
            for (int i = 0; i < threads.length; i++) {
                try {
                    threads[i].join();
                    HTTP.Response response = responses[i];
                    if (response == null || response.statusCode() != 200) {
                        throw new Exception("One/multiple POST requests did not return a 200 code");
                    }
                    String value = new String(responses[i].body());
                    Object lambdaObj = Serializer.byteArrayToObject(lambda, this.myJAR);
                    accumulator = ((FlamePairRDD.TwoStringsToString) lambdaObj).op(accumulator, value);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            return accumulator;
        }

        for (int i = 0; i < threads.length; i++) {
            try {
                threads[i].join();
                HTTP.Response response = responses[i];
                if (response == null || response.statusCode() != 200) {
                    throw new Exception("One/multiple POST requests did not return a 200 code");
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        return outputTableName;
    }

    private String createTableName() {
        return System.currentTimeMillis() + String.valueOf(Coordinator.nextJobID);
    }
}
