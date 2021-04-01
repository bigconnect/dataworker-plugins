package io.bigconnect.dw.sentiment.intellidockers;

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
import com.mware.ge.Visibility;
import com.mware.ge.mutation.ElementMutation;
import com.mware.ge.util.Preconditions;
import com.mware.ge.values.storable.StreamingPropertyValue;
import com.mware.ge.values.storable.Values;
import com.mware.ontology.IgnoredMimeTypes;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

@Name("IntelliDockers Sentiment Analysis")
@Description("Extracts sentiment from text using IntelliDockers")
public class IntelliDockersSentimentExtractorWorker extends DataWorker {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(IntelliDockersSentimentExtractorWorker.class);
    public static final String CONFIG_INTELLIDOCKERS_URL = "intellidockers.ron.sentiment.url";

    private IntelliDockersSentiment service;

    @Override
    public void prepare(DataWorkerPrepareData workerPrepareData) throws Exception {
        super.prepare(workerPrepareData);

        String url = getConfiguration().get(CONFIG_INTELLIDOCKERS_URL, null);
        Preconditions.checkState(!StringUtils.isEmpty(url), "Please provide the '" + CONFIG_INTELLIDOCKERS_URL + "' config parameter");

        Retrofit retrofit = new Retrofit.Builder()
                .baseUrl(url)
                .addConverterFactory(JacksonConverterFactory.create())
                .build();

        service = retrofit.create(IntelliDockersSentiment.class);
    }

    @Override
    public boolean isHandled(Element element, Property property) {
        if (property == null) {
            return false;
        }

        if (IgnoredMimeTypes.contains(BcSchema.MIME_TYPE.getFirstPropertyValue(element)))
            return false;

        if (property.getName().equals(RawObjectSchema.RAW_LANGUAGE.getPropertyName())) {
            // do entity extraction only if language is set
            String language = RawObjectSchema.RAW_LANGUAGE.getPropertyValue(property);
            return !StringUtils.isEmpty(language);
        }

        return false;
    }

    @Override
    public void execute(InputStream in, DataWorkerData data) throws Exception {
        String language = RawObjectSchema.RAW_LANGUAGE.getPropertyValue(data.getProperty());
        StreamingPropertyValue textProperty = BcSchema.TEXT.getPropertyValue(refresh(data.getElement()), data.getProperty().getKey());

        if (textProperty == null) {
            LOGGER.warn("Could not find text property for language: "+language);
            return;
        }

        String text = IOUtils.toString(textProperty.getInputStream(), StandardCharsets.UTF_8);

        ElementMutation m = refresh(data.getElement()).prepareMutation();
        m.deleteProperty(RawObjectSchema.RAW_SENTIMENT.getPropertyName(), Visibility.EMPTY);
        Element element = m.save(getAuthorizations());
        getGraph().flush();

        if (StringUtils.isEmpty(text)) {
            getWorkQueueRepository().pushGraphPropertyQueue(
                    element,
                    "",
                    RawObjectSchema.RAW_SENTIMENT.getPropertyName(),
                    data.getWorkspaceId(),
                    data.getVisibilitySource(),
                    data.getPriority(),
                    ElementOrPropertyStatus.DELETION,
                    null);
            pushTextUpdated(data);
            return;
        }

        try {
            Response<SentimentResponse> response = service.process(new SentimentRequest(text, "ron"))
                    .execute();
            if (response.isSuccessful() && response.body() != null) {
                String sentiment = toBcSentiment(response.body());
                m = element.prepareMutation();
                com.mware.ge.Metadata metadata = data.createPropertyMetadata(getUser());
                m.setProperty(RawObjectSchema.RAW_SENTIMENT.getPropertyName(), Values.stringValue(sentiment), metadata, data.getVisibility());
                element = m.save(getAuthorizations());

                getGraph().flush();
            }
        } catch (IOException e) {
            LOGGER.warn("Could not extract sentiment: %s", e.getMessage());
        }

        getWorkQueueRepository().pushGraphPropertyQueue(
                element,
                "",
                RawObjectSchema.RAW_SENTIMENT.getPropertyName(),
                data.getWorkspaceId(),
                data.getVisibilitySource(),
                data.getPriority(),
                ElementOrPropertyStatus.UPDATE,
                null);

        pushTextUpdated(data);
    }

    private String toBcSentiment(SentimentResponse sentiment) {
        switch (sentiment.label) {
            case "neg":
                return "negative";
            case "pos":
                return "positive";
            case "neu":
                return "neutral";
            default:
                throw new IllegalArgumentException("Unknown value: "+sentiment.label);
        }
    }
}
