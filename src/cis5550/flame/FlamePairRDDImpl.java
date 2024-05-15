package cis5550.flame;

import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Serializer;

import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class FlamePairRDDImpl implements FlamePairRDD {

    public String tableName;
    private final FlameContextImpl flameContext;

    public FlamePairRDDImpl(FlameContextImpl flameContext, String tableName) {
        this.flameContext = flameContext;
        this.tableName = tableName;
    }

    @Override
    public List<FlamePair> collect() throws Exception {
        List<FlamePair> list = new ArrayList<>();

        KVSClient kvsClient = this.flameContext.getKVS();
        Iterator<Row> it = kvsClient.scan(this.tableName);
        while (it.hasNext()) {
            Row row = it.next();
            for (String column : row.columns()) {
                FlamePair flamePair = new FlamePair(row.key(), row.get(column));
                list.add(flamePair);
            }
        }

        return list;
    }

    @Override
    public FlamePairRDD foldByKey(String zeroElement, TwoStringsToString lambda) throws Exception {
        String operationName = "/pairRdd/foldByKey";
        byte[] lambdaInBytes = Serializer.objectToByteArray(lambda);
        String zeroElementArg = "zeroElement=" + URLEncoder.encode(zeroElement);
        String outputTableName = this.flameContext.invokeOperation(
                operationName, lambdaInBytes, this.tableName, zeroElementArg);
        return new FlamePairRDDImpl(flameContext, outputTableName);
    }

    @Override
    public void saveAsTable(String tableNameArg) throws Exception {
        KVSClient kvsClient = this.flameContext.getKVS();
        System.out.println("this.tableName: ");
        System.out.println("this.tableName: " + this.tableName);
        kvsClient.rename(this.tableName, tableNameArg);
        this.tableName = tableNameArg;
    }

    @Override
    public FlameRDD flatMap(PairToStringIterable lambda) throws Exception {
        String operationName = "/pairRdd/flatMap";
        byte[] lambdaInBytes = Serializer.objectToByteArray(lambda);
        String outputTableName = this.flameContext.invokeOperation(operationName, lambdaInBytes, this.tableName);
        return new FlameRDDImpl(flameContext, outputTableName);
    }

    @Override
    public void destroy() throws Exception {
        KVSClient kvsClient = this.flameContext.getKVS();
        kvsClient.delete(this.tableName);
    }

    @Override
    public FlamePairRDD flatMapToPair(PairToPairIterable lambda) throws Exception {
        String operationName = "/pairRdd/flatMapToPair";
        byte[] lambdaInBytes = Serializer.objectToByteArray(lambda);
        String outputTableName = this.flameContext.invokeOperation(operationName, lambdaInBytes, this.tableName);
        return new FlamePairRDDImpl(flameContext, outputTableName);
    }

    @Override
    public FlamePairRDD join(FlamePairRDD other) throws Exception {
        String operationName = "/pairRdd/join";
        String otherTableArg = "otherTable=" + ((FlamePairRDDImpl) other).tableName;
        String outputTableName = this.flameContext.invokeOperation(
                operationName, null, this.tableName, otherTableArg);
        return new FlamePairRDDImpl(flameContext, outputTableName);
    }

    @Override
    public FlamePairRDD cogroup(FlamePairRDD other) throws Exception {
        return null;
    }
}
