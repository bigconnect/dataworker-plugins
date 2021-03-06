package io.bigconnect.dw.google.translate;

import com.google.common.base.Optional;
import com.optimaize.langdetect.LanguageDetectorBuilder;
import com.optimaize.langdetect.i18n.LdLocale;
import com.optimaize.langdetect.ngram.NgramExtractors;
import com.optimaize.langdetect.profiles.LanguageProfile;
import com.optimaize.langdetect.profiles.LanguageProfileReader;
import com.optimaize.langdetect.text.CommonTextObjectFactories;
import com.optimaize.langdetect.text.TextObject;
import com.optimaize.langdetect.text.TextObjectFactory;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

public class LanguageDetectorUtil {
    private com.optimaize.langdetect.LanguageDetector languageDetector;

    public LanguageDetectorUtil() throws Exception {
        List<LanguageProfile> languageProfiles = new LanguageProfileReader().readAllBuiltIn();
        languageDetector = LanguageDetectorBuilder.create(NgramExtractors.standard())
                .withProfiles(languageProfiles)
                .build();
    }

    synchronized Optional<String> detectLanguage(String text) {
        if (StringUtils.isEmpty(text))
            return Optional.absent();

        TextObjectFactory textObjectFactory = CommonTextObjectFactories.forDetectingOnLargeText();
        TextObject textObject = textObjectFactory.forText(text);
        Optional<LdLocale> lang = languageDetector.detect(textObject);
        return lang.transform(LdLocale::getLanguage);
    }
}
