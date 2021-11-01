package io.bigconnect.dw.text.common;

import com.github.pemistahl.lingua.api.IsoCode639_1;
import com.github.pemistahl.lingua.api.Language;
import com.github.pemistahl.lingua.api.LanguageDetector;
import com.github.pemistahl.lingua.api.LanguageDetectorBuilder;

import java.util.Optional;

public class LanguageDetectorUtil {
    private LanguageDetector languageDetector;

    public LanguageDetectorUtil() {
        languageDetector = LanguageDetectorBuilder
                .fromAllSpokenLanguages()
                .build();
    }

    public synchronized Optional<IsoCode639_1> detectLanguage(String text) {
        Language language = languageDetector.detectLanguageOf(text);
        return Optional.of(language)
                .map(Language::getIsoCode639_1);
    }
}
