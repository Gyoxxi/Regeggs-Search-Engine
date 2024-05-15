package cis5550.tools.autocomplete;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

/**
 * @author ericfouh
 */
public interface IAutocomplete
{

    /**
     * Adds a new word with its associated weight to the Trie.
     * If the word contains an invalid character, simply do nothing.
     * 
     * @param word the word to be added to the Trie
     * @param weight the weight of the word
     */
    public void addWord(String word, long weight);


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
    public Node buildTrie(String filename, int k) throws IOException;


    /**
     * @param prefix
     * @return the root of the subTrie corresponding to the last character of
     *         the prefix. If the prefix is not represented in the trie, return null.
     */
    public Node getSubTrie(String prefix);


    /**
     * @param prefix
     * @return the number of words that start with prefix.
     */
    public int countPrefixes(String prefix);



}
