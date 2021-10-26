package io.bigconnect.dw.text.language;

import com.mware.core.ingest.dataworker.DataWorkerPrepareData;
import com.mware.core.model.Description;
import com.mware.core.model.Name;
import io.bigconnect.dw.text.common.LanguageDetectorUtil;

import java.util.Optional;

@Name("Text Language Detector")
@Description("Detect the language of a piece of text")
public class BuiltInLanguageDetectorWorker extends LanguageDetectorWorkerBase {
    private LanguageDetectorUtil languageDetector;

    @Override
    public void prepare(DataWorkerPrepareData workerPrepareData) throws Exception {
        super.prepare(workerPrepareData);
        this.languageDetector = new LanguageDetectorUtil();
    }

    @Override
    public Optional<String> detectLanguage(String text) {
        return languageDetector.detectLanguage(text);
    }
}
