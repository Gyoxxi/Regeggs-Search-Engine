package cis5550.tools.autocomplete;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

public class Autocomplete implements IAutocomplete {

    private Node root = new Node();

    private int maxNum;

    public Autocomplete(int maxNum) {
        this.maxNum = maxNum;
        this.buildTrie("src/resources/words_alpha.txt", maxNum);
    }


    /**
     * Adds a new word with its associated weight to the Trie.
     * If the word contains an invalid character, simply do nothing.
     *
     * @param word the word to be added to the Trie
     * @param weight the weight of the word
     */
    @Override
    public void addWord(String word, long weight) {
        word = word.toLowerCase();
        Pattern p = Pattern.compile("[^a-z]");
        boolean containsInvalid = p.matcher(word).find();
        if (containsInvalid) {
            return;
        }

        Node curr = this.root;
        char[] array = word.toCharArray();
        curr.setPrefixes(curr.getPrefixes() + 1);

        for (int i = 0; i < array.length; i++) {
            char ch = array[i];
            // at this point, ch should be alphabet
            // 'a' is 97, corresponding to index 0
            int idx = (int) ch - 97;
            // if a child node not already exists at the index
            if (curr.getReferences()[idx] == null) {
                // for internal nodes
                if (i < array.length - 1) {
                    // store an empty node with empty term and 0 weight
                    curr.getReferences()[idx] = new Node();
                } else {
                    // for the terminal node
                    // store a node with complete word and weight
                    curr.getReferences()[idx] = new Node(word, weight);
                }
            }
            // after updating child node, or already has child node
            Node child = curr.getReferences()[idx];

            // if this is the terminal node, then we have a complete word
            if (i == array.length - 1) {
                child.setWords(1);
            }

            // update number of prefixes + 1
            // a newly created node will have 1
            // an existing node will have prefix = prefix + 1
            child.setPrefixes(child.getPrefixes() + 1);

            // move to child node
            curr = child;
        }

    }


    /**
     * Initializes the Trie
     *
     * @param filename the file to read all the autocomplete data from each line
     *                 contains a word and its weight This method will call the
     *                 addWord method
     * @param k the maximum number of suggestions that should be displayed
     * @return the root of the Trie You might find the readLine() method in
     *         BufferedReader useful in this situation as it will allow you to
     *         read a file one line at a time.
     */
    @Override
    public Node buildTrie(String filename, int k) {
        this.root = new Node();
        try {
            // read the file
            BufferedReader br = new BufferedReader(new FileReader(filename));
            this.maxNum = k;
            while (br.ready()) {
                String word = br.readLine().trim();
                // add this word to trie
                addWord(word, 0);
            }
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return this.root;
    }


    /**
     *
     * @param prefix the prefix of the node
     * @return the root of the subTrie corresponding to the last character of
     *         the prefix. If the prefix is not represented in the trie, return null.
     */
    @Override
    public Node getSubTrie(String prefix) {
        // check if the prefix contains only alphabets
        prefix = prefix.toLowerCase();
        Pattern p = Pattern.compile("[^a-z]");
        boolean containsInvalid = p.matcher(prefix).find();
        if (containsInvalid) {
            return null;
        }

        // start at the root node
        Node curr = this.root;
        char[] array = prefix.toCharArray();
        for (char ch : array) {
            int idx = (int) ch - 97;
            // if required char does not exist in child nodes
            // return null
            if (curr.getReferences()[idx] == null) {
                return null;
            }
            curr = curr.getReferences()[idx];
        }

        // at this point we have traversed all char nodes
        // curr node is the last char of the prefix
        return curr;
    }


    /**
     * @return the number of words that start with prefix.
     */
    @Override
    public int countPrefixes(String prefix) {
        // try to get the subTrie root
        Node res = getSubTrie(prefix);
        if (res == null) {
            return 0;
        }
        return res.getPrefixes();
    }

    /**
     * This method should not throw an exception.
     * Make sure that you preserve encapsulation by returning a list of copies of the
     * original Terms; otherwise, the user might be able to change the structure of your
     * Trie based on the values returned.
     *
     * @param prefix the prefix of the node
     * @return a List containing all the ITerm objects with query starting with
     *         prefix. Return an empty list if there are no ITerm object starting
     *         with prefix.
     */
    public Set<String> getSuggestions(String prefix) {
        List<ITerm> ret = new ArrayList<>();

        // try to move to the root of subTrie
        Node curr = getSubTrie(prefix);
        if (curr == null) {
            return new TreeSet<>();
        }

        // dfs from the root of subTrie
        // update list when we found a word
        dfs(curr, ret);

        return selectEvenlySpacedTerms(ret, maxNum);
    }

    private void dfs(Node node, List<ITerm> lst) {
        // if a word is found
        // add the corresponding Term object to list
        if (node.getWords() == 1) {
            lst.add(new Term(node.getTerm().getTerm(), node.getTerm().getWeight()));
        }
        for (Node c : node.getReferences()) {
            if (c != null) {
                dfs(c, lst);
            }
        }
    }

    private Set<String> selectEvenlySpacedTerms(List<ITerm> prefixList, int maxCount) {
        Set<String> selectedTerms = new TreeSet<>();
        if (prefixList.isEmpty() || maxCount <= 0) {
            return selectedTerms;
        }
        // Calculate the step to pick terms evenly
        double step = (double) prefixList.size() / maxCount;
        for (int i = 0; i < maxCount && i * step < prefixList.size(); i++) {
            selectedTerms.add(prefixList.get((int) Math.floor(i * step)).getTerm());
        }

        return selectedTerms;
    }
}
