package io.bigconnect.dw.sentiment.intellidockers;

import com.mware.core.exception.BcException;
import com.mware.core.ingest.dataworker.DataWorker;
import com.mware.core.ingest.dataworker.DataWorkerData;
import com.mware.core.ingest.dataworker.DataWorkerPrepareData;
import com.mware.core.ingest.dataworker.ElementOrPropertyStatus;
import com.mware.core.model.Description;
import com.mware.core.model.Name;
import com.mware.core.model.clientapi.dto.VisibilityJson;
import com.mware.core.model.properties.BcSchema;
import com.mware.core.model.properties.RawObjectSchema;
import com.mware.core.model.termMention.TermMentionBuilder;
import com.mware.core.model.termMention.TermMentionRepository;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.ge.Element;
import com.mware.ge.Property;
import com.mware.ge.Vertex;
import com.mware.ge.Visibility;
import com.mware.ge.metric.PausableTimerContext;
import com.mware.ge.metric.Timer;
import com.mware.ge.mutation.ElementMutation;
import com.mware.ge.util.Preconditions;
import com.mware.ge.values.storable.StreamingPropertyValue;
import com.mware.ge.values.storable.Values;
import com.mware.ontology.IgnoredMimeTypes;
import io.bigconnect.dw.text.common.NerUtils;
import io.bigconnect.dw.text.common.TextSpan;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import com.fasterxml.jackson.databind.ObjectMapper;

import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

@Name("Sentiment Analysis for Romanian")
@Description("Extracts sentiment from Romanian text")
public class IntelliDockersSentimentExtractorWorker extends DataWorker {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(IntelliDockersSentimentExtractorWorker.class);

    public static final String CONFIG_URL = "fastapi.api.url";
    public static final String CONFIG_API_KEY = "vllm.api.key";
    public static final String CONFIG_TIMEOUT = "sentiment.timeout.seconds";
    public static final String CONFIG_PARAGRAPHS = "sentiment.ron.paragraphs";

    private static final int DEFAULT_TIMEOUT_SECONDS = 30;
    private static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");

    private OkHttpClient client;
    private String apiUrl;
    private boolean doParagraphs;
    private TermMentionRepository termMentionRepository;
    private Timer detectTimer;
    private ObjectMapper objectMapper;

    @Inject
    public IntelliDockersSentimentExtractorWorker(TermMentionRepository termMentionRepository) {
        this.termMentionRepository = termMentionRepository;
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public void prepare(DataWorkerPrepareData workerPrepareData) throws Exception {
        super.prepare(workerPrepareData);

        String baseUrl = getConfiguration().get(CONFIG_URL, null);
        String apiKey = getConfiguration().get(CONFIG_API_KEY, null);
        int timeoutSeconds = Integer.parseInt(getConfiguration().get(CONFIG_TIMEOUT, String.valueOf(DEFAULT_TIMEOUT_SECONDS)));

        Preconditions.checkState(!StringUtils.isEmpty(baseUrl), "Please provide the '" + CONFIG_URL + "' config parameter");

        this.apiUrl = baseUrl + "/sentiment";

        OkHttpClient.Builder clientBuilder = new OkHttpClient.Builder()
                .connectTimeout(timeoutSeconds, TimeUnit.SECONDS)
                .readTimeout(timeoutSeconds, TimeUnit.SECONDS)
                .writeTimeout(timeoutSeconds, TimeUnit.SECONDS);

        if (!StringUtils.isEmpty(apiKey)) {
            clientBuilder.addInterceptor(chain -> chain.proceed(
                    chain.request().newBuilder()
                            .header("Authorization", "Bearer " + apiKey)
                            .build()
            ));
        }

        this.client = clientBuilder.build();
        this.doParagraphs = getConfiguration().getBoolean(CONFIG_PARAGRAPHS, false);
        this.detectTimer = getGraph().getMetricsRegistry().getTimer(getClass(), "sentiment-time");
    }

    @Override
    public boolean isHandled(Element element, Property property) {
        if (property == null) return false;
        if (IgnoredMimeTypes.contains(BcSchema.MIME_TYPE.getFirstPropertyValue(element))) return false;

        // Check if sentiment property already exists - don't process if it does
        Property sentimentProperty = element.getProperty(RawObjectSchema.RAW_SENTIMENT.getPropertyName());
        if (sentimentProperty != null) {
            LOGGER.debug("Sentiment property already exists for element: {}, skipping", element.getId());
            return false;
        }

        if (property.getName().equals(RawObjectSchema.RAW_LANGUAGE.getPropertyName())) {
            String language = RawObjectSchema.RAW_LANGUAGE.getPropertyValue(property);
            return !StringUtils.isEmpty(language);
        }
        return false;
    }

    @Override
    public void execute(InputStream in, DataWorkerData data) throws Exception {
        Property textProperty = BcSchema.TEXT.getProperty(refresh(data.getElement()), data.getProperty().getKey());
        if (textProperty == null) {
            LOGGER.warn("Could not find text property");
            return;
        }

        StreamingPropertyValue spv = BcSchema.TEXT.getPropertyValue(textProperty);
        if (spv == null) {
            LOGGER.warn("Could not find text property value");
            return;
        }

        String text = IOUtils.toString(spv.getInputStream(), StandardCharsets.UTF_8);
        if (StringUtils.isEmpty(text)) {
            clearSentiment(data);
            return;
        }

        try {
            if (doParagraphs) {
                processParagraphs(text, data, textProperty);
            } else {
                processSingleText(text, data);
            }
        } catch (IOException e) {
            LOGGER.warn("Could not extract sentiment: %s", e.getMessage());
        }
    }

    private void processSingleText(String text, DataWorkerData data) throws Exception {
        PausableTimerContext timer = new PausableTimerContext(detectTimer);
        try {
            Map<String, Object> requestBody = new HashMap<>();
            requestBody.put("text", text);
            requestBody.put("dev", false);

            String jsonRequest = objectMapper.writeValueAsString(requestBody);
            RequestBody body = RequestBody.create(JSON, jsonRequest);

            Request request = new Request.Builder()
                    .url(apiUrl)
                    .post(body)
                    .build();

            try (Response response = client.newCall(request).execute()) {
                if (response.isSuccessful() && response.body() != null) {
                    String responseJson = response.body().string();
                    Map<String, Object> responseMap = objectMapper.readValue(responseJson, Map.class);
                    String sentiment = (String) responseMap.get("sentiment");

                    Vertex vertex = (Vertex)refresh(data.getElement());
                    String propertyName = RawObjectSchema.RAW_SENTIMENT.getPropertyName();

                    // Remove existing property before setting new value
                    vertex.getProperties(propertyName).forEach(p ->
                            vertex.softDeleteProperty(p.getKey(), propertyName, p.getVisibility(), getAuthorizations())
                    );

                    vertex.addPropertyValue(data.getProperty().getKey(), propertyName,
                            Values.stringValue(sentiment.toLowerCase()),
                            data.createPropertyMetadata(getUser()),
                            data.getVisibility(),
                            getAuthorizations());

                    getGraph().flush();
                    pushWorkQueueUpdate(data);
                }
            }
        } finally {
            timer.close();
        }
    }

    private void processParagraphs(String text, DataWorkerData data, Property textProperty) throws IOException {
        NerUtils.removeSentimentTermMentions((Vertex)refresh(data.getElement()), termMentionRepository, getGraph(), getAuthorizations());
        List<TextSpan> paragraphs = NerUtils.getSmartMiniLMChunks(text);

        VisibilityJson tmVisibilityJson = new VisibilityJson();
        tmVisibilityJson.setSource("");

        int positiveCount = 0;
        int negativeCount = 0;
        int neutralCount = 0;

        for (TextSpan p : paragraphs) {
            Map<String, Object> requestBody = new HashMap<>();
            requestBody.put("text", p.getText());
            requestBody.put("dev", false);

            String jsonRequest = objectMapper.writeValueAsString(requestBody);
            RequestBody body = RequestBody.create(JSON, jsonRequest);

            Request request = new Request.Builder()
                    .url(apiUrl)
                    .post(body)
                    .build();

            try (Response response = client.newCall(request).execute()) {
                if (response.isSuccessful() && response.body() != null) {
                    String responseJson = response.body().string();
                    Map<String, Object> responseMap = objectMapper.readValue(responseJson, Map.class);
                    String sentiment = ((String) responseMap.get("sentiment")).toLowerCase();
                    double score = 1.0; // Default score since the API doesn't provide scores

                    TermMentionBuilder tmb = new TermMentionBuilder()
                            .outVertex((Vertex)refresh(data.getElement()))
                            .propertyKey(textProperty.getKey())
                            .propertyName(textProperty.getName())
                            .start(p.getStart())
                            .end(p.getEnd())
                            .title(String.format("%s", StringUtils.capitalize(sentiment)))
                            .score(score)
                            .type("sent")
                            .visibilityJson(tmVisibilityJson)
                            .process(getClass().getName());

                    if ("positive".equals(sentiment)) {
                        tmb.style("background-color: rgba(0, 255, 0, 0.3);");
                        positiveCount++;
                    } else if ("negative".equals(sentiment)) {
                        tmb.style("background-color: rgba(255, 0, 0, 0.3);");
                        negativeCount++;
                    } else {
                        neutralCount++;
                    }

                    tmb.save(getGraph(), getVisibilityTranslator(), getUser(), getAuthorizations());
                }
            }
        }

        String overallSentiment = calculateOverallSentiment(positiveCount, negativeCount, neutralCount);
        ElementMutation<Vertex> m = ((Vertex)refresh(data.getElement())).prepareMutation();
        m.setProperty(RawObjectSchema.RAW_SENTIMENT.getPropertyName(),
                Values.stringValue(overallSentiment),
                data.createPropertyMetadata(getUser()),
                data.getVisibility());
        m.save(getAuthorizations());
        getGraph().flush();

        pushWorkQueueUpdate(data);
    }

    private String calculateOverallSentiment(int positive, int negative, int neutral) {
        if (positive > negative && positive > neutral) return "positive";
        if (negative > positive && negative > neutral) return "negative";
        return "neutral";
    }

    private void clearSentiment(DataWorkerData data) {
        ElementMutation<Vertex> m = refresh(data.getElement()).prepareMutation();
        m.deleteProperty(RawObjectSchema.RAW_SENTIMENT.getPropertyName(), Visibility.EMPTY);
        m.save(getAuthorizations());
        getGraph().flush();
        pushWorkQueueUpdate(data);
    }

    private void pushWorkQueueUpdate(DataWorkerData data) {
        getWorkQueueRepository().pushOnDwQueue(
                refresh(data.getElement()),
                "",
                RawObjectSchema.RAW_SENTIMENT.getPropertyName(),
                data.getWorkspaceId(),
                data.getVisibilitySource(),
                data.getPriority(),
                ElementOrPropertyStatus.UPDATE,
                null
        );
        pushTextUpdated(data);
    }
}