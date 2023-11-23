package io.bigconnect.dw.text.summary;

import com.mware.core.ingest.dataworker.DataWorker;
import com.mware.core.ingest.dataworker.DataWorkerData;
import com.mware.core.ingest.dataworker.DataWorkerPrepareData;
import com.mware.core.model.Description;
import com.mware.core.model.Name;
import com.mware.core.model.properties.BcSchema;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.ge.Element;
import com.mware.ge.Property;
import com.mware.ge.Vertex;
import com.mware.ge.Visibility;
import com.mware.ge.mutation.ElementMutation;
import com.mware.ge.util.Preconditions;
import com.mware.ge.values.storable.StreamingPropertyValue;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static io.bigconnect.dw.text.common.TextPropertyHelper.getTextPropertyForLanguage;
import static io.bigconnect.dw.text.summary.SummarySchemaContribution.SUMMARY;

@Name("Text summarization for Romanian")
@Description("Text summarization for Romanian text")
public class TextSummaryDetectorWorker extends DataWorker {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(TextSummaryDetectorWorker.class);

    public static final String CONFIG_URL = "summary.ron.url";

    SummaryService service;

    @Override
    public void prepare(DataWorkerPrepareData workerPrepareData) throws Exception {
        super.prepare(workerPrepareData);

        String url = getConfiguration().get(CONFIG_URL, null);
        Preconditions.checkState(!StringUtils.isEmpty(url), "Please provide the '" + CONFIG_URL + "' config parameter");
        Retrofit retrofit = new Retrofit.Builder()
                .baseUrl(url)
                .addConverterFactory(JacksonConverterFactory.create())
                .build();

        service = retrofit.create(SummaryService.class);
    }

    @Override
    public boolean isHandled(Element element, Property property) {
        if (property == null) {
            return false;
        }

        if (SUMMARY.getPropertyName().equals(property.getName())) {
            String summary = SUMMARY.getPropertyValue(element);
            return StringUtils.isEmpty(summary);
        }

        return false;
    }

    @Override
    public void execute(InputStream in, DataWorkerData data) throws Exception {
        Property textProperty = getTextPropertyForLanguage(data.getElement(), "ro")
                .orElse(BcSchema.TEXT.getFirstProperty(data.getElement()));

        if (textProperty == null) {
            LOGGER.warn("Could not find text property");
            return;
        }

        StreamingPropertyValue spv = BcSchema.TEXT.getPropertyValue(textProperty);
        if (spv == null) {
            LOGGER.warn("text property is null");
            return;
        }

        String text = IOUtils.toString(spv.getInputStream(), StandardCharsets.UTF_8);

        if (StringUtils.isEmpty(text)) {
            return;
        }

        try {
            Response<SummaryResponse> response = service.process(new SummaryRequest(text)).execute();
            SummaryResponse result = response.body();
            if (result != null && StringUtils.isNotEmpty(result.summary)) {
                ElementMutation<Vertex> m = refresh(data.getElement()).prepareMutation();
                SUMMARY.setProperty(m, result.summary, Visibility.EMPTY);
                m.save(getAuthorizations());
                data.getElement().getGraph().flush();
            }
        } catch (IOException e) {
            LOGGER.warn("Could not extract text summary: %s", e.getMessage());
        }
    }
}
