package cis5550.flame;

import java.io.Serializable;
import java.util.List;

public interface FlamePairRDD {
    List<FlamePair> collect() throws Exception;

    FlamePairRDD foldByKey(String zeroElement, TwoStringsToString lambda) throws Exception;

    void saveAsTable(String tableNameArg) throws Exception;

    // collect() should return a list that contains all the elements in the PairRDD.

    FlameRDD flatMap(PairToStringIterable lambda) throws Exception;

    // foldByKey() folds all the values that are associated with a given key in the
    // current PairRDD, and returns a new PairRDD with the resulting keys and values.
    // Formally, the new PairRDD should contain a pair (k,v) for each distinct key k
    // in the current PairRDD, where v is computed as follows: Let v_1,...,v_N be the
    // values associated with k in the current PairRDD (in other words, the current
    // PairRDD contains (k,v_1),(k,v_2),...,(k,v_N)). Then the provided lambda should
    // be invoked once for each v_i, with that v_i as the second argument. The first
    // invocation should use 'zeroElement' as its first argument, and each subsequent
    // invocation should use the result of the previous one. v is the result of the
    // last invocation.

    void destroy() throws Exception;

    // saveAsTable() should cause a table with the specified name to appear
    // in the KVS that contains the data from this PairRDD. The table should
    // have a row for each unique key in the PairRDD, and the different values
    // that are associated with this key should be in different columns. The
    // names of the columns can be anything.

    FlamePairRDD flatMapToPair(PairToPairIterable lambda) throws Exception;

    // flatMap() should invoke the provided lambda once for each pair in the PairRDD,
    // and it should return a new RDD that contains all the strings from the Iterables
    // the lambda invocations have returned. It is okay for the same string to appear
    // more than once in the output; in this case, the RDD should contain multiple
    // copies of that string. The lambda is allowed to return null or an empty Iterable.

    FlamePairRDD join(FlamePairRDD other) throws Exception;

    // destroy() should delete the underlying table in the key-value store.
    // Any future invocations of any method on this RDD should throw an
    // exception.

    FlamePairRDD cogroup(FlamePairRDD other) throws Exception;

    // flatMapToPair() is analogous to flatMap(), except that the lambda returns pairs
    // instead of strings, and tha tthe output is a PairRDD instead of a normal RDD.

    interface TwoStringsToString extends Serializable {
        String op(String a, String b);
    }

    // join() joins the current PairRDD A with another PairRDD B. Suppose A contains
    // a pair (k,v_A) and B contains a pair (k,v_B). Then the result should contain
    // a pair (k,v_A+","+v_B).

    interface PairToPairIterable extends Serializable {
        Iterable<FlamePair> op(FlamePair a) throws Exception;
    }

    // This method should return a new PairRDD that contains, for each key k that exists
    // in either the original RDD or in R, a pair (k,"[X],[Y]"), where X and Y are
    // comma-separated lists of the values from the original RDD and from R, respectively.
    // For instance, if the original RDD contains (fruit,apple) and (fruit,banana) and
    // R contains (fruit,cherry), (fruit,date) and (fruit,fig), the result should contain
    // a pair with key fruit and value [apple,banana],[cherry,date,fig]. This method is
    // extra credit in HW7; if you do not implement it, please return 'null'.

    interface PairToStringIterable extends Serializable {
        Iterable<String> op(FlamePair a) throws Exception;
    }
}
