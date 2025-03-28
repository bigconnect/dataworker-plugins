package io.bigconnect.dw.classification.iptc.intellidockersVllm;

import com.mware.core.ingest.dataworker.DataWorker;
import com.mware.core.ingest.dataworker.DataWorkerData;
import com.mware.core.ingest.dataworker.DataWorkerPrepareData;
import com.mware.core.model.Description;
import com.mware.core.model.Name;
import com.mware.core.model.properties.BcSchema;
import com.mware.core.model.properties.RawObjectSchema;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.ge.Element;
import com.mware.ge.Property;
import com.mware.ge.mutation.ElementMutation;
import com.mware.ge.util.Preconditions;
import com.mware.ge.values.storable.*;
import com.mware.ontology.IgnoredMimeTypes;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import static io.bigconnect.dw.classification.iptc.intellidockersVllm.IntelliDockersIptcSchemaContribution.IPTC;
import static io.bigconnect.dw.classification.iptc.intellidockersVllm.IntelliDockersIptcSchemaContribution.TOPICS;


@Name("Text Analysis Worker")
@Description("Analyzes text using NLP services")
public class IntelliDockersIptcExtractorWorkerVllm extends DataWorker {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(IntelliDockersIptcExtractorWorkerVllm.class);

    // Configuration constants
    public static final String CONFIG_URL = "vllm.url";
    public static final String CONFIG_API_PATH = "vllm.nlp.path";
    public static final String CONFIG_API_KEY = "vllm.api.key";
    public static final String CONFIG_TIMEOUT = "ocr.timeout.seconds";
    private static final int DEFAULT_TIMEOUT_SECONDS = 30;

    private IntelliDockersIptc service;

    @Override
    public void prepare(DataWorkerPrepareData workerPrepareData) throws Exception {
        super.prepare(workerPrepareData);

        String baseUrl = getConfiguration().get(CONFIG_URL, null);
        String apiPath = getConfiguration().get(CONFIG_API_PATH, null);
        String apiKey = getConfiguration().get(CONFIG_API_KEY, null);
        int timeoutSeconds;
        try {
            timeoutSeconds = Integer.parseInt(getConfiguration().get(CONFIG_TIMEOUT, String.valueOf(DEFAULT_TIMEOUT_SECONDS)));
        } catch (NumberFormatException e) {
            LOGGER.warn("Invalid timeout value in configuration, using default: " + DEFAULT_TIMEOUT_SECONDS);
            timeoutSeconds = DEFAULT_TIMEOUT_SECONDS;
        }

        Preconditions.checkState(!StringUtils.isEmpty(baseUrl),
                "Please provide the '" + CONFIG_URL + "' config parameter");

        OkHttpClient.Builder clientBuilder = new OkHttpClient.Builder()
                .connectTimeout(timeoutSeconds, TimeUnit.SECONDS)
                .readTimeout(timeoutSeconds, TimeUnit.SECONDS)
                .writeTimeout(timeoutSeconds, TimeUnit.SECONDS);

        // Add API key if configured
        if (!StringUtils.isEmpty(apiKey)) {
            clientBuilder.addInterceptor(chain -> {
                Request original = chain.request();
                Request request = original.newBuilder()
                        .header("Authorization", "Bearer " + apiKey)
                        .method(original.method(), original.body())
                        .build();
                return chain.proceed(request);
            });
        }

        Retrofit retrofit = new Retrofit.Builder()
                .baseUrl(baseUrl)
                .client(clientBuilder.build())
                .addConverterFactory(JacksonConverterFactory.create())
                .build();

        service = retrofit.create(IntelliDockersIptc.class);
    }

    @Override
    public boolean isHandled(Element element, Property property) {
        if (property == null) {
            return false;
        }

        if (IgnoredMimeTypes.contains(BcSchema.MIME_TYPE.getFirstPropertyValue(element)))
            return false;

        if (property.getName().equals(RawObjectSchema.RAW_LANGUAGE.getPropertyName())) {
            String language = RawObjectSchema.RAW_LANGUAGE.getPropertyValue(property);
            return !StringUtils.isEmpty(language);
        }

        return false;
    }

    @Override
    public void execute(InputStream in, DataWorkerData data) throws Exception {
        Element element = refresh(data.getElement());
        Property textProperty = BcSchema.TEXT.getProperty(element, data.getProperty().getKey());
        StreamingPropertyValue spv = BcSchema.TEXT.getPropertyValue(textProperty);
        String language = RawObjectSchema.RAW_LANGUAGE.getFirstPropertyValue(data.getElement());

        if (spv == null) {
            LOGGER.warn("Could not find text property");
            return;
        }

        String text = IOUtils.toString(spv.getInputStream(), StandardCharsets.UTF_8);
        if (StringUtils.isEmpty(text)) {
            return;
        }

        try {
            TextAnalysisRequest request = new TextAnalysisRequest(text, "classify", language);
            Response<TextAnalysisResponse> response = service.processText(request).execute();

            if (response.isSuccessful() && response.body() != null) {
                TextAnalysisResponse result = response.body();
                ElementMutation<?> m = element.prepareMutation();

                // Handle classification results
                if (result.classification != null && result.classification.classifications != null) {
                    // Create a string builder or list to collect all categories
                    StringBuilder categories = new StringBuilder();

                    // Iterate through classifications and build comma-separated string
                    for (Classification classification : result.classification.classifications) {
                        if (categories.length() > 0) {
                            categories.append(", ");
                        }
                        categories.append(classification.category);
                    }

                    // Add single property value with all categories
                    if (categories.length() > 0) {
                        m.addPropertyValue(
                                data.getProperty().getKey(),
                                IPTC.getPropertyName(),
                                Values.stringValue(categories.toString()),
                                data.getVisibility()
                        );
                    }
                }

                // Handle topics
                if (result.topics != null && !result.topics.isEmpty()) {
                    String[] topicsArray = result.topics.toArray(new String[0]);
                    m.addPropertyValue(data.getProperty().getKey(), TOPICS.getPropertyName(),
                            Values.stringArray(topicsArray), data.getVisibility());
                }

                m.save(getAuthorizations());
                getGraph().flush();
            }
        } catch (IOException e) {
            LOGGER.warn("Could not analyze text: %s", e.getMessage());
        }
    }
}
