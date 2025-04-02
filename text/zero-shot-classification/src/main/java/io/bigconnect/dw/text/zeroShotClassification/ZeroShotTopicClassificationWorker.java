package io.bigconnect.dw.text.zeroShotClassification;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mware.core.ingest.dataworker.DataWorker;
import com.mware.core.ingest.dataworker.DataWorkerData;
import com.mware.core.ingest.dataworker.DataWorkerPrepareData;
import com.mware.core.ingest.dataworker.ElementOrPropertyStatus;
import com.mware.core.model.Description;
import com.mware.core.model.Name;
import com.mware.core.model.properties.BcSchema;
import com.mware.core.model.properties.RawObjectSchema;
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
import okhttp3.*;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.bigconnect.dw.text.zeroShotClassification.ZeroShotClassificationSchemaContribution.OTHER_RELEVANT_KEYWORDS_PROPERTY;
import static io.bigconnect.dw.text.zeroShotClassification.ZeroShotClassificationSchemaContribution.TOPIC_CLASSIFICATION_PROPERTY_PREFIX;

@Name("Zero-Shot Topic Classification")
@Description("Classifies text content using FastAPI Zero-Shot Classification endpoint")
public class ZeroShotTopicClassificationWorker extends DataWorker {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(ZeroShotTopicClassificationWorker.class);

    public static final String CONFIG_URL = "fastapi.api.url";
    public static final String API_PATH = "fastapi.api.classification";
    public static final String CONFIG_API_KEY = "vllm.api.key";
    public static final String CONFIG_DEV_MODE = "devmode.classifier";
    private boolean devMode;

    public static final String CONFIG_TIMEOUT = "zeroshotclassifier.timeout.seconds";

    private static final int DEFAULT_TIMEOUT_SECONDS = 50000;
    private static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");
    private static final int MAX_CLASSIFICATIONS = 10;

    private OkHttpClient client;
    private String apiUrl;
    private Timer classificationTimer;
    private ObjectMapper objectMapper;

    @Inject
    public ZeroShotTopicClassificationWorker() {
        this.objectMapper = new ObjectMapper();
        LOGGER.info("ZeroShotTopicClassificationWorker initialized with ObjectMapper");
    }

    @Override
    public void prepare(DataWorkerPrepareData workerPrepareData) throws Exception {
        super.prepare(workerPrepareData);
        LOGGER.info("Preparing ZeroShotTopicClassificationWorker");

        String baseUrl = getConfiguration().get(CONFIG_URL, null);
        String path = getConfiguration().get(API_PATH, null);
        this.devMode = Boolean.parseBoolean(getConfiguration().get(CONFIG_DEV_MODE, "false"));
        String apiKey = getConfiguration().get(CONFIG_API_KEY, null);
        int timeoutSeconds = Integer.parseInt(getConfiguration().get(CONFIG_TIMEOUT, String.valueOf(DEFAULT_TIMEOUT_SECONDS)));

        LOGGER.info("Configuration loaded: baseUrl=" + (baseUrl != null ? baseUrl : "null") +
                ", apiKey=" + (apiKey != null ? "provided" : "null") +
                ", timeoutSeconds=" + timeoutSeconds);

        Preconditions.checkState(!StringUtils.isEmpty(baseUrl), "Please provide the '" + CONFIG_URL + "' config parameter");

        this.apiUrl = baseUrl + path;
        LOGGER.info("API URL configured as: " + this.apiUrl);

        OkHttpClient.Builder clientBuilder = new OkHttpClient.Builder()
                .connectTimeout(timeoutSeconds, TimeUnit.SECONDS)
                .readTimeout(timeoutSeconds, TimeUnit.SECONDS)
                .writeTimeout(timeoutSeconds, TimeUnit.SECONDS);

        if (!StringUtils.isEmpty(apiKey)) {
            LOGGER.info("Adding API key authorization header");
            clientBuilder.addInterceptor(chain -> chain.proceed(
                    chain.request().newBuilder()
                            .header("Authorization", "Bearer " + apiKey)
                            .build()
            ));
        }

        this.client = clientBuilder.build();
        LOGGER.info("HTTP client created with timeout: " + timeoutSeconds + " seconds");

        try {
            if (getGraph() != null && getGraph().getMetricsRegistry() != null) {
                this.classificationTimer = getGraph().getMetricsRegistry().getTimer(getClass(), "topic-classification-time");
                LOGGER.info("Classification timer initialized successfully");
            } else {
                LOGGER.warn("Unable to initialize classification timer - graph or metrics registry is null");
            }
        } catch (Exception e) {
            LOGGER.warn("Failed to initialize classification timer", e);
        }

        LOGGER.info("ZeroShotTopicClassificationWorker preparation completed successfully");
    }

    @Override
    public boolean isHandled(Element element, Property property) {
        if (property == null) {
            LOGGER.debug("Property is null, not handling element: " + element.getId());
            return false;
        }

        String mimeType = BcSchema.MIME_TYPE.getFirstPropertyValue(element);
        if (IgnoredMimeTypes.contains(mimeType)) {
            LOGGER.debug("Ignored mime type: " + mimeType + ", not handling element: " + element.getId());
            return false;
        }

        Property topicClassificationProperty = element.getProperty(TOPIC_CLASSIFICATION_PROPERTY_PREFIX + "1");
        if (topicClassificationProperty != null) {
            LOGGER.debug("topicClassification1 property already exists, not handling element: " + element.getId());
            return false;
        }

        // Check if otherRelevantKeywords property is null or empty
        if (!devMode) {
            // Check if otherRelevantKeywords property is null or empty
            Property otherRelevantKeywordsProperty = element.getProperty(OTHER_RELEVANT_KEYWORDS_PROPERTY);
            if (otherRelevantKeywordsProperty == null ||
                    StringUtils.isEmpty(otherRelevantKeywordsProperty.getValue().toString())) {
                LOGGER.debug("otherRelevantKeywords is null or empty, not handling element: " + element.getId());
                return false;
            }
        }

        // Check for text property
        if (property.getName().equals(RawObjectSchema.RAW_LANGUAGE.getPropertyName())) {
            String language = RawObjectSchema.RAW_LANGUAGE.getPropertyValue(property);
            boolean isHandled = !StringUtils.isEmpty(language);
            LOGGER.debug("Element: " + element.getId() +
                    ", Property: " + property.getName() +
                    ", Language: " + language +
                    ", IsHandled: " + isHandled);
            return isHandled;
        }

        LOGGER.debug("Not handling element: " + element.getId() + ", property: " + property.getName());
        return false;
    }

    @Override
    public void execute(InputStream in, DataWorkerData data) throws Exception {
        LOGGER.info("Executing classification for element: " + data.getElement().getId() +
                ", property: " + data.getProperty().getName());

        Element refreshedElement = refresh(data.getElement());
        LOGGER.debug("Element refreshed: " + refreshedElement.getId());

        Property textProperty = BcSchema.TEXT.getProperty(refreshedElement, data.getProperty().getKey());
        if (textProperty == null) {
            LOGGER.warn("Could not find text property for element: " + refreshedElement.getId() +
                    ", property key: " + data.getProperty().getKey());
            return;
        }
        LOGGER.debug("Found text property: " + textProperty.getName());

        StreamingPropertyValue spv = BcSchema.TEXT.getPropertyValue(textProperty);
        if (spv == null) {
            LOGGER.warn("Could not find text property value for element: " + refreshedElement.getId());
            return;
        }
        LOGGER.debug("Found streaming property value");

        String text;
        try {
            text = IOUtils.toString(spv.getInputStream(), StandardCharsets.UTF_8);
            LOGGER.debug("Text extracted, length: " + text.length() + " characters");
        } catch (Exception e) {
            LOGGER.error("Error reading text from property value", e);
            return;
        }

        if (StringUtils.isEmpty(text)) {
            LOGGER.warn("Text is empty for element: " + refreshedElement.getId() + ", clearing classification results");
            clearClassificationResults(data);
            return;
        }

        try {
            LOGGER.info("Calling zero-shot classification API for element: " + data.getElement().getId());
            callZeroShotClassificationAPI(text, data);
        } catch (IOException e) {
            LOGGER.error("Could not perform zero-shot classification", e);
        }
    }

    private void clearClassificationResults(DataWorkerData data) {
        LOGGER.info("Clearing classification results for element: " + data.getElement().getId());

        try {
            Element refreshedElement = refresh(data.getElement());
            LOGGER.debug("Element refreshed for clearing: " + refreshedElement.getId());

            ElementMutation<Vertex> m = ((Vertex) refreshedElement).prepareMutation();
            LOGGER.debug("Element mutation prepared for clearing properties");

            clearAllClassificationProperties(m);
            LOGGER.debug("Classification properties cleared in mutation");

            m.save(getAuthorizations());
            LOGGER.info("Cleared properties mutation saved");

            getGraph().flush();
            LOGGER.debug("Graph flushed after clearing properties");

            pushWorkQueueUpdate(data);
            LOGGER.info("Work queue updates pushed after clearing properties");
        } catch (Exception e) {
            LOGGER.error("Error clearing classification results", e);
        }
    }

    private void callZeroShotClassificationAPI(String text, DataWorkerData data) throws Exception {
        PausableTimerContext timer = null;
        long startTime = System.currentTimeMillis();

        try {
            LOGGER.info("Starting classification API call for element: " + data.getElement().getId());
            // Only create timer if classificationTimer is properly initialized
            if (classificationTimer != null) {
                timer = new PausableTimerContext(classificationTimer);
                LOGGER.debug("Timer context created");
            }

            Map<String, Object> requestBody = new HashMap<>();
            requestBody.put("text", text);
            requestBody.put("dev", false);

            String jsonRequest = objectMapper.writeValueAsString(requestBody);
            LOGGER.debug("Request body created, length: " + jsonRequest.length());

            RequestBody body = RequestBody.create(JSON, jsonRequest);
            Request request = new Request.Builder()
                    .url(apiUrl)
                    .post(body)
                    .build();
            LOGGER.info("Sending request to API: " + apiUrl);

            long requestSentTime = System.currentTimeMillis();
            try (Response response = client.newCall(request).execute()) {
                long responseReceivedTime = System.currentTimeMillis();
                LOGGER.info("Received API response in " +
                        (responseReceivedTime - requestSentTime) +
                        " ms, status code: " + response.code());

                if (response.isSuccessful() && response.body() != null) {
                    String responseJson = response.body().string();
                    LOGGER.debug("Response body received, length: " + responseJson.length() + " characters");

                    try {
                        // Create a proper class for the response structure
                        JsonNode rootNode = objectMapper.readTree(responseJson);
                        LOGGER.debug("Response parsed to JSON, first 500 chars: " +
                                rootNode.toString().substring(0, Math.min(500, rootNode.toString().length())));

                        if (rootNode.has("classification_results")) {
                            LOGGER.info("Found classification_results in response");
                            JsonNode classificationResults = rootNode.get("classification_results");

                            if (classificationResults.isArray()) {
                                LOGGER.info("Classification results is an array with " +
                                        classificationResults.size() + " items");

                                Vertex vertex = (Vertex) refresh(data.getElement());
                                LOGGER.debug("Element refreshed for updating: " + vertex.getId());

                                ElementMutation<Vertex> m = vertex.prepareMutation();
                                LOGGER.debug("Element mutation prepared");

                                // Clear previous classifications
                                clearAllClassificationProperties(m);
                                LOGGER.debug("Previous classification properties cleared");

                                int resultCount = Math.min(classificationResults.size(), MAX_CLASSIFICATIONS);
                                LOGGER.info("Processing " + resultCount + " classification results");

                                int savedCount = 0;
                                for (int i = 0; i < resultCount; i++) {
                                    JsonNode result = classificationResults.get(i);
                                    LOGGER.debug("Processing result " + i + ": " + result);

                                    if (result.has("overall_classification")) {
                                        String classification = result.get("overall_classification").asText();
                                        LOGGER.debug("Result " + i + " has overall_classification: " + classification);

                                        // Only save non-empty classifications
                                        if (StringUtils.isNotBlank(classification)) {
                                            // Save using index+1 (1-based for property names)
                                            String propertyName = getClassificationPropertyName(i + 1);
                                            LOGGER.debug("Setting property " + propertyName + ": " + classification);

                                            m.setProperty(propertyName,
                                                    Values.stringValue(classification),
                                                    data.createPropertyMetadata(getUser()),
                                                    data.getVisibility());
                                            savedCount++;
                                        } else {
                                            LOGGER.warn("Result " + i + " has empty classification value, skipping");
                                        }
                                    } else {
                                        LOGGER.warn("Result " + i + " missing overall_classification field");
                                    }
                                }

                                // Only save if we have at least one valid classification
                                if (savedCount > 0) {
                                    LOGGER.info("Saving mutation with " + savedCount + " classification properties");
                                    try {
                                        m.save(getAuthorizations());
                                        LOGGER.info("Mutation saved successfully");
                                    } catch (Exception e) {
                                        LOGGER.error("Error saving mutation", e);
                                    }

                                    try {
                                        getGraph().flush();
                                        LOGGER.info("Graph flushed successfully");
                                    } catch (Exception e) {
                                        LOGGER.error("Error flushing graph", e);
                                    }

                                    pushWorkQueueUpdate(data);
                                    LOGGER.info("Work queue updates pushed");
                                } else {
                                    LOGGER.warn("No valid classification results found, not saving anything");
                                }
                            } else {
                                LOGGER.warn("classification_results is not an array: " + classificationResults);
                            }
                        } else if (rootNode.has("error")) {
                            LOGGER.error("API returned error: " + rootNode.get("error"));
                        } else {
                            LOGGER.warn("Response missing classification_results field");
                        }
                    } catch (Exception e) {
                        LOGGER.error("Error processing API response", e);
                        LOGGER.debug("Response that caused error, length: " + responseJson.length());
                    }
                } else {
                    String errorBody = response.body() != null ? response.body().string() : "null";
                    LOGGER.error("Zero-shot classification API call failed with code: " +
                            response.code() + ", body: " + errorBody);
                }
            }
        } finally {
            // Only close if timer was created and not null
            if (timer != null) {
                try {
                    timer.close();
                    LOGGER.debug("Timer context closed");
                } catch (Exception e) {
                    LOGGER.warn("Error closing timer", e);
                }
            }

            long endTime = System.currentTimeMillis();
            LOGGER.info("ZeroShot classification completed in " +
                    (endTime - startTime) + " ms for element: " + data.getElement().getId());
        }
    }

    private String getClassificationPropertyName(int index) {
        return TOPIC_CLASSIFICATION_PROPERTY_PREFIX + index;
    }

    private void clearAllClassificationProperties(ElementMutation<Vertex> m) {
        for (int i = 1; i <= MAX_CLASSIFICATIONS; i++) {
            m.deleteProperty(getClassificationPropertyName(i), Visibility.EMPTY);
        }
    }

    private void pushWorkQueueUpdate(DataWorkerData data) {
        // Push updates for all potential classification properties
        for (int i = 1; i <= MAX_CLASSIFICATIONS; i++) {
            getWorkQueueRepository().pushOnDwQueue(
                    refresh(data.getElement()),
                    "",
                    getClassificationPropertyName(i),
                    data.getWorkspaceId(),
                    data.getVisibilitySource(),
                    data.getPriority(),
                    ElementOrPropertyStatus.UPDATE,
                    null
            );
        }
    }
}