package cis5550.test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class WordsTest {
    public static void main(String[] args) {
        String fileName = "src/resources/words.txt";
        int maxLength = 0;
        String maxString = "";

        try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.length() > maxLength) {
                    maxLength = line.length();
                    maxString = line.trim();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("The longest line's character count is: " + maxLength + ", " + maxString);
    }
}

