package io.bigconnect.dw.text.language.intellidockers;

import com.github.pemistahl.lingua.api.IsoCode639_3;
import com.github.pemistahl.lingua.api.Language;
import com.mware.core.ingest.dataworker.DataWorkerPrepareData;
import com.mware.core.model.Description;
import com.mware.core.model.Name;
import com.mware.ge.util.Preconditions;
import io.bigconnect.dw.text.language.LanguageDetectorWorkerBase;
import org.apache.commons.lang3.StringUtils;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;

import java.io.IOException;
import java.util.Optional;

@Name("IntelliDockers Language Detector")
@Description("Detect the language of a piece of text using IntelliDockers")
public class IntelliDockersLanguageDetectorWorker extends LanguageDetectorWorkerBase {
    public static final String CONFIG_INTELLIDOCKERS_URL = "intellidockers.language.url";

    IntelliDockersLanguage languageDetector;

    @Override
    public void prepare(DataWorkerPrepareData workerPrepareData) throws Exception {
        super.prepare(workerPrepareData);

        String url = getConfiguration().get(CONFIG_INTELLIDOCKERS_URL, null);
        Preconditions.checkState(!StringUtils.isEmpty(url), "Please provide the '" + CONFIG_INTELLIDOCKERS_URL + "' config parameter");
        Retrofit retrofit = new Retrofit.Builder()
                .baseUrl(url)
                .addConverterFactory(JacksonConverterFactory.create())
                .build();

        languageDetector = retrofit.create(IntelliDockersLanguage.class);
    }

    @Override
    public Optional<String> detectLanguage(String text) {
        try {
            Response<IdLanguageResponse> response = languageDetector.process(new IdLanguageRequest(text))
                    .execute();
            if (response.isSuccessful() && response.body() != null) {
                String threeLetterLanguage = response.body().language;
                return Optional.of(
                        Language.getByIsoCode639_3(IsoCode639_3.valueOf(threeLetterLanguage.toUpperCase()))
                                .getIsoCode639_1().toString()
                );
            }
        } catch (IOException e) {
            LOGGER.warn("Could not detect language: %s", e.getMessage());
        }

        return Optional.empty();
    }
}
