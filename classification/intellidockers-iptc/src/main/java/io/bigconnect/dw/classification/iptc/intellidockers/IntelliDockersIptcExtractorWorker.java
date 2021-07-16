package io.bigconnect.dw.classification.iptc.intellidockers;

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
import com.mware.ge.*;
import com.mware.ge.mutation.ElementMutation;
import com.mware.ge.util.Preconditions;
import com.mware.ge.values.storable.StreamingPropertyValue;
import com.mware.ge.values.storable.Values;
import com.mware.ontology.IgnoredMimeTypes;
import io.bigconnect.dw.text.common.TextPropertyHelper;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

import static io.bigconnect.dw.classification.iptc.intellidockers.IntelliDockersIptcSchemaContribution.IPTC;
import static io.bigconnect.dw.classification.iptc.intellidockers.IntelliDockersIptcSchemaContribution.IPTC_SCORE;

@Name("IntelliDockers IPTC Extractor")
@Description("Extracts categories from text using IntelliDockers")
public class IntelliDockersIptcExtractorWorker extends DataWorker {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(IntelliDockersIptcExtractorWorker.class);
    public static final String CONFIG_INTELLIDOCKERS_URL = "intellidockers.ron.iptc.url";

    private IntelliDockersIptc service;

    @Override
    public void prepare(DataWorkerPrepareData workerPrepareData) throws Exception {
        super.prepare(workerPrepareData);

        String url = getConfiguration().get(CONFIG_INTELLIDOCKERS_URL, null);
        Preconditions.checkState(!StringUtils.isEmpty(url), "Please provide the '" + CONFIG_INTELLIDOCKERS_URL + "' config parameter");

        Retrofit retrofit = new Retrofit.Builder()
                .baseUrl(url)
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
            // do entity extraction only if language is set
            String language = RawObjectSchema.RAW_LANGUAGE.getPropertyValue(property);
            return !StringUtils.isEmpty(language) && "ro".equals(language);
        }

        return false;
    }

    @Override
    public void execute(InputStream in, DataWorkerData data) throws Exception {
        Element element = refresh(data.getElement());
        String language = RawObjectSchema.RAW_LANGUAGE.getPropertyValue(data.getProperty());
        Property textProperty = BcSchema.TEXT.getProperty(element, data.getProperty().getKey());
        StreamingPropertyValue spv = BcSchema.TEXT.getPropertyValue(textProperty);

        if (spv == null) {
            LOGGER.warn("Could not find text property for language: "+language);
            return;
        }

        String text = IOUtils.toString(spv.getInputStream(), StandardCharsets.UTF_8);
        if (StringUtils.isEmpty(text)) {
            return;
        }

        try {
            Response<IptcResponse> response = service.process(new IptcRequest(text, "ron"))
                    .execute();
            if (response.isSuccessful() && response.body() != null) {
                // remove previous values
                ElementMutation m = element.prepareMutation();
                m.deleteProperty(IPTC.getPropertyName(), Visibility.EMPTY);
                element = m.save(getAuthorizations());
                getGraph().flush();

                // set new classes
                m = element.prepareMutation();
                List<IptcResponse.IptcCategory> categories = response.body().categories;
                for (IptcResponse.IptcCategory category : categories) {
                    com.mware.ge.Metadata metadata = data.createPropertyMetadata(getUser());
                    IPTC_SCORE.setMetadata(metadata, category.score, Visibility.EMPTY);
                    IPTC.addPropertyValue(m, category.label, category.label, metadata, Visibility.EMPTY);
                }
                Element e = m.save(getAuthorizations());

                getGraph().flush();

                getWorkQueueRepository().pushGraphPropertyQueue(
                        e,
                        null,
                        IPTC.getPropertyName(),
                        data.getWorkspaceId(),
                        data.getVisibilitySource(),
                        data.getPriority(),
                        ElementOrPropertyStatus.UPDATE,
                        null);
            }
        } catch (IOException e) {
            LOGGER.warn("Could not extract sentiment: %s", e.getMessage());
        }
    }
}
