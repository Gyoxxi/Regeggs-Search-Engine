package cis5550.tools.autocomplete;

import java.util.Comparator;

/**
 * @author ericfouh
 */
public interface ITerm
    extends Comparable<ITerm> {

    /**
     * Compares the two terms in descending order by weight.
     * 
     * @return comparator Object
     */
    public static Comparator<ITerm> byReverseWeightOrder() {
        return new Comparator<ITerm>() {
            @Override
            public int compare(ITerm o1, ITerm o2) {
                return Long.compare(o2.getWeight(), o1.getWeight());
            }
        };
    }


    /**
     * Compares the two terms in lexicographic order but using only the first r
     * characters of each query.
     * 
     * @param r
     * @return comparator Object
     */
    public static Comparator<ITerm> byPrefixOrder(int r) {
        if (r < 0) {
            throw new IllegalArgumentException();
        }
        return new Comparator<ITerm>() {
            @Override
            public int compare(ITerm o1, ITerm o2) {
                int l1 = o1.getTerm().length();
                int l2 = o2.getTerm().length();
                int nr = Math.min(r, Math.min(l1, l2));
                String s1 = o1.getTerm().substring(0, nr);
                String s2 = o2.getTerm().substring(0, nr);
                return s1.compareTo(s2);
            }
        };
    }

    // Compares the two terms in lexicographic order by query.
    public int compareTo(ITerm that);


    // Returns a string representation of this term in the following format:
    // the weight, followed by a tab, followed by the query.
    public String toString();

    // Required getters.
    public long getWeight();
    public String getTerm();

    // Required setters (mostly for autograding purposes)
    public void setWeight(long weight);
    public void setTerm(String term);

}
