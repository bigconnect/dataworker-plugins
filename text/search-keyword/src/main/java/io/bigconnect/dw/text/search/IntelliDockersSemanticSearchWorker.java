package io.bigconnect.dw.text.search;

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
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.bigconnect.dw.text.search.SemanticSearchSchemaContribution.OTHER_RELEVANT_BUCKETS;
import static io.bigconnect.dw.text.search.SemanticSearchSchemaContribution.OTHER_RELEVANT_KEYWORDS;


@Name("Semantic Search")
@Description("Performs semantic search using FastAPI endpoint")
public class IntelliDockersSemanticSearchWorker extends DataWorker {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(IntelliDockersSemanticSearchWorker.class);
    public static final String CONFIG_DEV_MODE = "devmode.classifier";
    private boolean devMode;


    public static final String CONFIG_URL = "fastapi.api.url";
    public static final String CONFIG_API_KEY = "vllm.api.key";
    public static final String CONFIG_TIMEOUT = "semantic.timeout.seconds";

    private static final int DEFAULT_TIMEOUT_SECONDS = 30;
    private static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");

    private OkHttpClient client;
    private String apiUrl;
    private Timer searchTimer;
    private ObjectMapper objectMapper;

    @Inject
    public IntelliDockersSemanticSearchWorker() {
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public void prepare(DataWorkerPrepareData workerPrepareData) throws Exception {
        super.prepare(workerPrepareData);
        this.devMode = Boolean.parseBoolean(getConfiguration().get(CONFIG_DEV_MODE, "false"));
        String baseUrl = getConfiguration().get(CONFIG_URL, null);
        String apiKey = getConfiguration().get(CONFIG_API_KEY, null);
        int timeoutSeconds = Integer.parseInt(getConfiguration().get(CONFIG_TIMEOUT, String.valueOf(DEFAULT_TIMEOUT_SECONDS)));

        Preconditions.checkState(!StringUtils.isEmpty(baseUrl), "Please provide the '" + CONFIG_URL + "' config parameter");

        this.apiUrl = baseUrl + "/semantic/combined-search";

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
        this.searchTimer = getGraph().getMetricsRegistry().getTimer(getClass(), "semantic-search-time");
    }

    @Override
    public boolean isHandled(Element element, Property property) {
        if (devMode) {
            LOGGER.debug("Dev mode enabled, skipping element: {}", element.getId());
            return false;
        }

        if (property == null) return false;
        if (IgnoredMimeTypes.contains(BcSchema.MIME_TYPE.getFirstPropertyValue(element))) return false;

        // Check if otherRelevantKeywords property exists
        Property keywordsProperty = element.getProperty(OTHER_RELEVANT_KEYWORDS.getPropertyName());
        if (keywordsProperty != null) {
            LOGGER.debug("otherRelevantKeywords property not present, not handling element: {}", element.getId());
            return false;
        }

        // Check for text property
        if (property.getName().equals(RawObjectSchema.RAW_LANGUAGE.getPropertyName())) {
            String language = RawObjectSchema.RAW_LANGUAGE.getPropertyValue(property);
            return !StringUtils.isEmpty(language);
        }

        return false;
    }

    @Override
    public void execute(InputStream in, DataWorkerData data) throws Exception {
        String query = null;
        String keyword = null;

        // Try to get serpKeyword first
        Property serpKeywordProperty = RawObjectSchema.SERP_KEYWORD.getProperty(refresh(data.getElement()));
        if (serpKeywordProperty != null) {
            keyword = RawObjectSchema.SERP_KEYWORD.getPropertyValue(serpKeywordProperty);
        }

        // If no serpKeyword found, try text content
        StringUtils.isEmpty(query);
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

        query = IOUtils.toString(spv.getInputStream(), StandardCharsets.UTF_8);
        if (StringUtils.isEmpty(query)) {
            clearSemanticResults(data);
            return;
        }

        try {
            callSemanticSearchAPI(query, data);
        } catch (IOException e) {
            LOGGER.warn("Could not perform semantic search: %s", e.getMessage());
        }
    }

    private void callSemanticSearchAPI(String query, DataWorkerData data) throws Exception {
        PausableTimerContext timer = new PausableTimerContext(searchTimer);
        try {
            Map<String, Object> requestBody = new HashMap<>();
            requestBody.put("query", query);
            requestBody.put("limit", 10);
            requestBody.put("threshold", 0.3);
            requestBody.put("search_keywords", true);
            requestBody.put("search_buckets", true);

            String jsonRequest = objectMapper.writeValueAsString(requestBody);
            RequestBody body = RequestBody.create(MediaType.parse("application/json"), jsonRequest);
            Request request = new Request.Builder()
                    .url(apiUrl)
                    .post(body)
                    .build();

            try (Response response = client.newCall(request).execute()) {
                if (response.isSuccessful() && response.body() != null) {
                    String responseJson = response.body().string();
                    Map<String, Object> responseMap = objectMapper.readValue(responseJson, Map.class);

                    // Extract the lists from the response
                    Vertex vertex = (Vertex) refresh(data.getElement());
                    ElementMutation<Vertex> m = vertex.prepareMutation();

                    StringBuilder keywordsText = new StringBuilder();
                    if (responseMap.containsKey("similar_keywords")) {
                        List<String> similarKeywords = (List<String>) responseMap.get("similar_keywords");
                        if (similarKeywords != null && !similarKeywords.isEmpty()) {
                            boolean first = true;
                            for (String keyword : similarKeywords) {
                                if (!first) {
                                    keywordsText.append(", ");
                                } else {
                                    first = false;
                                }
                                keywordsText.append(keyword);
                            }
                        }
                    }

                    StringBuilder bucketsText = new StringBuilder();
                    if (responseMap.containsKey("similar_buckets")) {
                        List<String> similarBuckets = (List<String>) responseMap.get("similar_buckets");
                        if (similarBuckets != null && !similarBuckets.isEmpty()) {
                            boolean first = true;
                            for (String bucket : similarBuckets) {
                                if (!first) {
                                    bucketsText.append(", ");
                                } else {
                                    first = false;
                                }
                                bucketsText.append(bucket);
                            }
                        }
                    }

                    // Save the extracted keywords using the schema property
                    m.setProperty(OTHER_RELEVANT_KEYWORDS.getPropertyName(),
                            Values.stringValue(keywordsText.toString()),
                            data.createPropertyMetadata(getUser()),
                            data.getVisibility());

                    // Save the extracted buckets using the schema property
                    m.setProperty(OTHER_RELEVANT_BUCKETS.getPropertyName(),
                            Values.stringValue(bucketsText.toString()),
                            data.createPropertyMetadata(getUser()),
                            data.getVisibility());

                    m.save(getAuthorizations());
                    getGraph().flush();
                    pushWorkQueueUpdate(data);
                } else {
                    LOGGER.warn("Semantic search API call failed with code: %d", response.code());
                }
            }
        } finally {
            timer.close();
        }
    }

    private void clearSemanticResults(DataWorkerData data) {
        ElementMutation<Vertex> m = refresh(data.getElement()).prepareMutation();
        m.deleteProperty(OTHER_RELEVANT_KEYWORDS.getPropertyName(), Visibility.EMPTY);
        m.deleteProperty(OTHER_RELEVANT_BUCKETS.getPropertyName(), Visibility.EMPTY);
        m.save(getAuthorizations());
        getGraph().flush();
        pushWorkQueueUpdate(data);
    }

    private void pushWorkQueueUpdate(DataWorkerData data) {
        // Update for the keywords property
        getWorkQueueRepository().pushOnDwQueue(
                refresh(data.getElement()),
                "",
                OTHER_RELEVANT_KEYWORDS.getPropertyName(),
                data.getWorkspaceId(),
                data.getVisibilitySource(),
                data.getPriority(),
                ElementOrPropertyStatus.UPDATE,
                null
        );

        // Update for the buckets property
        getWorkQueueRepository().pushOnDwQueue(
                refresh(data.getElement()),
                "",
                OTHER_RELEVANT_BUCKETS.getPropertyName(),
                data.getWorkspaceId(),
                data.getVisibilitySource(),
                data.getPriority(),
                ElementOrPropertyStatus.UPDATE,
                null
        );
    }
}