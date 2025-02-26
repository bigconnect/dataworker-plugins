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
import okhttp3.OkHttpClient;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

@Name("Sentiment Analysis for Romanian")
@Description("Extracts sentiment from Romanian text")
public class IntelliDockersSentimentExtractorWorker extends DataWorker {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(IntelliDockersSentimentExtractorWorker.class);

    public static final String CONFIG_URL = "vllm.url";
    public static final String CONFIG_API_KEY = "vllm.api.key";
    public static final String CONFIG_TIMEOUT = "ocr.timeout.seconds";
    public static final String CONFIG_PARAGRAPHS = "sentiment.ron.paragraphs";

    private static final int DEFAULT_TIMEOUT_SECONDS = 30;

    private IntelliDockersSentiment service;
    private boolean doParagraphs;
    private TermMentionRepository termMentionRepository;
    private Timer detectTimer;

    @Inject
    public IntelliDockersSentimentExtractorWorker(TermMentionRepository termMentionRepository) {
        this.termMentionRepository = termMentionRepository;
    }

    @Override
    public void prepare(DataWorkerPrepareData workerPrepareData) throws Exception {
        super.prepare(workerPrepareData);

        String baseUrl = getConfiguration().get(CONFIG_URL, null);
        String apiKey = getConfiguration().get(CONFIG_API_KEY, null);
        int timeoutSeconds = Integer.parseInt(getConfiguration().get(CONFIG_TIMEOUT, String.valueOf(DEFAULT_TIMEOUT_SECONDS)));

        Preconditions.checkState(!StringUtils.isEmpty(baseUrl), "Please provide the '" + CONFIG_URL + "' config parameter");

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

        Retrofit retrofit = new Retrofit.Builder()
                .baseUrl(baseUrl)
                .client(clientBuilder.build())
                .addConverterFactory(JacksonConverterFactory.create())
                .build();

        service = retrofit.create(IntelliDockersSentiment.class);
        this.doParagraphs = getConfiguration().getBoolean(CONFIG_PARAGRAPHS, false);
        this.detectTimer = getGraph().getMetricsRegistry().getTimer(getClass(), "sentiment-time");
    }

    @Override
    public boolean isHandled(Element element, Property property) {
        if (property == null) return false;
        if (IgnoredMimeTypes.contains(BcSchema.MIME_TYPE.getFirstPropertyValue(element))) return false;

        if (property.getName().equals(RawObjectSchema.RAW_LANGUAGE.getPropertyName())) {
            String language = RawObjectSchema.RAW_LANGUAGE.getPropertyValue(property);
            return !StringUtils.isEmpty(language) && "ro".equals(language);
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
            TextAnalysisRequest request = new TextAnalysisRequest(text, "sentiment");
            Response<TextAnalysisResponse> response = service.processText(request).execute();

            if (response.isSuccessful() && response.body() != null && response.body().sentiment != null) {
                Vertex vertex = (Vertex)refresh(data.getElement());
                String propertyName = RawObjectSchema.RAW_SENTIMENT.getPropertyName();

                // Remove existing property before setting new value
                vertex.getProperties(propertyName).forEach(p ->
                        vertex.softDeleteProperty(p.getKey(), propertyName, p.getVisibility(), getAuthorizations())
                );

                String sentiment = extractSentiment(response.body().sentiment);
                vertex.addPropertyValue(data.getProperty().getKey(), propertyName,
                        Values.stringValue(sentiment),
                        data.createPropertyMetadata(getUser()),
                        data.getVisibility(),
                        getAuthorizations());

                getGraph().flush();
                pushWorkQueueUpdate(data);
            }
        } finally {
            timer.close();
        }
    }

    private void processParagraphs(String text, DataWorkerData data, Property textProperty) throws IOException {
        NerUtils.removeSentimentTermMentions((Vertex)refresh(data.getElement()), termMentionRepository, getGraph(), getAuthorizations());
        List<TextSpan> paragraphs = NerUtils.getParagraphs(text);

        VisibilityJson tmVisibilityJson = new VisibilityJson();
        tmVisibilityJson.setSource("");

        int positiveCount = 0;
        int negativeCount = 0;
        int neutralCount = 0;

        for (TextSpan p : paragraphs) {
            TextAnalysisRequest request = new TextAnalysisRequest(p.getText(), "sentiment");
            Response<TextAnalysisResponse> response = service.processText(request).execute();

            if (response.isSuccessful() && response.body() != null && response.body().sentiment != null) {
                Map<String, Object> sentimentResult = response.body().sentiment;
                String sentiment = extractSentiment(sentimentResult);
                double score = extractScore(sentimentResult);

                TermMentionBuilder tmb = new TermMentionBuilder()
                        .outVertex((Vertex)refresh(data.getElement()))
                        .propertyKey(textProperty.getKey())
                        .propertyName(textProperty.getName())
                        .start(p.getStart())
                        .end(p.getEnd())
                        .title(String.format("%s: %f", StringUtils.capitalize(sentiment), score))
                        .score(score)
                        .type("sent")
                        .visibilityJson(tmVisibilityJson)
                        .process(getClass().getName());

                if ("positive".equals(sentiment)) {
                    tmb.style(String.format("background-color: rgba(0, 255, 0, %f);", score / 3));
                    positiveCount++;
                } else if ("negative".equals(sentiment)) {
                    tmb.style(String.format("background-color: rgba(255, 0, 0, %f);", score / 3));
                    negativeCount++;
                } else {
                    neutralCount++;
                }

                tmb.save(getGraph(), getVisibilityTranslator(), getUser(), getAuthorizations());
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

    private String extractSentiment(Map<String, Object> sentimentResult) {
        if (sentimentResult == null) return "neutral";

        try {
            Map<String, Double> scores = (Map<String, Double>) sentimentResult.get("scores");
            if (scores == null) return "neutral";

            double positive = scores.getOrDefault("positive", 0.0);
            double negative = scores.getOrDefault("negative", 0.0);
            double neutral = scores.getOrDefault("neutral", 0.0);

            if (positive > negative && positive > neutral) return "positive";
            if (negative > positive && negative > neutral) return "negative";
            return "neutral";
        } catch (Exception e) {
            LOGGER.warn("Error extracting sentiment", e);
            return "neutral";
        }
    }

    private double extractScore(Map<String, Object> sentimentResult) {
        try {
            Map<String, Double> scores = (Map<String, Double>) sentimentResult.get("scores");
            if (scores == null) return 0.0;

            return Math.max(
                    Math.max(
                            scores.getOrDefault("positive", 0.0),
                            scores.getOrDefault("negative", 0.0)
                    ),
                    scores.getOrDefault("neutral", 0.0)
            );
        } catch (Exception e) {
            LOGGER.warn("Error extracting score", e);
            return 0.0;
        }
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