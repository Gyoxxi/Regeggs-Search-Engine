package cis5550.flame;

import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Serializer;

import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

public class FlameRDDImpl implements FlameRDD {

    private final FlameContextImpl flameContext;
    private String tableName;

    public FlameRDDImpl(FlameContextImpl flameContext, String tableName) {
        this.flameContext = flameContext;
        this.tableName = tableName;
    }

    @Override
    public int count() throws Exception {
        KVSClient kvsClient = this.flameContext.getKVS();
        return kvsClient.count(this.tableName);
    }

    @Override
    public void saveAsTable(String tableNameArg) throws Exception {
        KVSClient kvsClient = this.flameContext.getKVS();
        kvsClient.rename(this.tableName, tableNameArg);
        this.tableName = tableNameArg;
    }

    @Override
    public FlameRDD distinct() throws Exception {
        String operationName = "/rdd/distinct";
        String outputTableName = this.flameContext.invokeOperation(operationName, null, this.tableName);
        return new FlameRDDImpl(flameContext, outputTableName);
    }

    @Override
    public void destroy() throws Exception {
        KVSClient kvsClient = this.flameContext.getKVS();
        kvsClient.delete(this.tableName);
    }

    @Override
    public Vector<String> take(int num) throws Exception {
        Vector<String> firstNumRow = new Vector<>();

        KVSClient kvsClient = this.flameContext.getKVS();
        Iterator<Row> it = kvsClient.scan(this.tableName);
        while (it.hasNext()) {
            Row row = it.next();
            firstNumRow.add(row.get("value"));
            if (firstNumRow.size() == num) {
                break;
            }
        }

        return firstNumRow;
    }

    @Override
    public String fold(String zeroElement, FlamePairRDD.TwoStringsToString lambda) throws Exception {
        String operationName = "/rdd/fold";
        byte[] lambdaInBytes = Serializer.objectToByteArray(lambda);
        String zeroElementArg = "zeroElement=" + URLEncoder.encode(zeroElement);
        String isFoldOperationArg = "isFoldOperation=true";
        return this.flameContext.invokeOperation(
                operationName, lambdaInBytes, this.tableName, zeroElementArg, isFoldOperationArg);
    }

    @Override
    public List<String> collect() throws Exception {
        List<String> list = new ArrayList<>();

        KVSClient kvsClient = this.flameContext.getKVS();
        Iterator<Row> it = kvsClient.scan(this.tableName);
        while (it.hasNext()) {
            Row row = it.next();
            String element = row.get("value");
            list.add(element);
        }

        return list;
    }

    @Override
    public FlameRDD flatMap(StringToIterable lambda) throws Exception {
        String operationName = "/rdd/flatMap";
        byte[] lambdaInBytes = Serializer.objectToByteArray(lambda);
        String outputTableName = this.flameContext.invokeOperation(operationName, lambdaInBytes, this.tableName);
        return new FlameRDDImpl(flameContext, outputTableName);
    }

    @Override
    public FlamePairRDD flatMapToPair(StringToPairIterable lambda) throws Exception {
        String operationName = "/rdd/flatMapToPair";
        byte[] lambdaInBytes = Serializer.objectToByteArray(lambda);
        String outputTableName = this.flameContext.invokeOperation(operationName, lambdaInBytes, this.tableName);
        return new FlamePairRDDImpl(flameContext, outputTableName);
    }

    @Override
    public FlamePairRDD mapToPair(StringToPair lambda) throws Exception {
        String operationName = "/rdd/mapToPair";
        byte[] lambdaInBytes = Serializer.objectToByteArray(lambda);
        String outputTableName = this.flameContext.invokeOperation(operationName, lambdaInBytes, this.tableName);
        return new FlamePairRDDImpl(flameContext, outputTableName);
    }

    @Override
    public FlameRDD intersection(FlameRDD r) throws Exception {
        return null;
    }

    @Override
    public FlameRDD sample(double f) throws Exception {
        return null;
    }

    @Override
    public FlamePairRDD groupBy(StringToString lambda) throws Exception {
        return null;
    }

    @Override
    public FlameRDD filter(StringToBoolean lambda) throws Exception {
        String operationName = "/rdd/filter";
        byte[] lambdaInBytes = Serializer.objectToByteArray(lambda);
        String outputTableName = this.flameContext.invokeOperation(operationName, lambdaInBytes, this.tableName);
        return new FlameRDDImpl(flameContext, outputTableName);
    }

    @Override
    public FlameRDD mapPartitions(IteratorToIterator lambda) throws Exception {
        return null;
    }
}
