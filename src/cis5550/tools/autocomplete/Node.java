package cis5550.tools.autocomplete;

import java.util.Arrays;

/**
 * @author Harry Smith
 */

public class Node {

    private Term term;

    // whether this node represents a complete word, 0 or 1
    private int words = 0;

    // how many words have the prefix of the node
    private int prefixes = 0;

    private Node[] references;

    /**
     * Initialize a Node with an empty string and 0 weight; useful for
     * writing tests.
     */
    public Node() {
        this.term = new Term("", 0);
        this.references = new Node[26];
        Arrays.fill(this.references, null);
    }

    /**
     * Initialize a Node with the given query string and weight.
     * @throws IllegalArgumentException if query is null or if weight is negative.
     */
    public Node(String query, long weight) {
        if (query == null || weight < 0) {
            throw new IllegalArgumentException();
        }

        this.term = new Term(query, weight);
        this.references = new Node[26];
        Arrays.fill(this.references, null);
    }

    public Term getTerm() {
        return term;
    }

    public void setTerm(Term term) {
        this.term = term;
    }

    public int getWords() {
        return words;
    }

    public void setWords(int words) {
        this.words = words;
    }

    public int getPrefixes() {
        return prefixes;
    }

    public void setPrefixes(int prefixes) {
        this.prefixes = prefixes;
    }

    public Node[] getReferences() {
        return references;
    }

    public void setReferences(Node[] references) {
        this.references = references;
    }
}
