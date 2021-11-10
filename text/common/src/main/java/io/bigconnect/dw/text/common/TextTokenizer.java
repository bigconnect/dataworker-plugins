package io.bigconnect.dw.text.common;

import com.github.pemistahl.lingua.api.IsoCode639_3;
import com.mware.ge.io.IOUtils;
import opennlp.tools.tokenize.WhitespaceTokenizer;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TextTokenizer {

    public static List<String> tokenize(String text) {
        String[] parts = WhitespaceTokenizer.INSTANCE.tokenize(text);
        return Stream.of(parts).map(s -> {
                    StringBuilder res = new StringBuilder();
                    for (Character c : s.toCharArray()) {
                        if (Character.isLetterOrDigit(c))
                            res.append(c);
                    }
                    return res.toString();
                }).filter(s -> !s.isEmpty())
                .collect(Collectors.toList());
    }

    /**
     * Removes stopwords while keeping the original case
     */
    public static List<String> removeStopwords(List<String> tokens, IsoCode639_3 lang) {
        InputStream is = TextTokenizer.class.getResourceAsStream("/stopwords/" + lang.toString());
        if (is == null) {
            return new ArrayList<>(tokens);
        }

        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        Set<String> stopWords = new HashSet<>();
        reader.lines().forEach(stopWords::add);
        IOUtils.closeAllSilently(reader);

        List<String> result = new ArrayList<>(tokens);
        result.removeIf(t -> stopWords.contains(t.toLowerCase()));
        return result;
    }
}
