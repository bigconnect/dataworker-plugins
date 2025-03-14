package io.bigconnect.dw.text.summary;

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
import com.mware.ge.Vertex;
import com.mware.ge.Visibility;
import com.mware.ge.mutation.ElementMutation;
import com.mware.ge.util.Preconditions;
import com.mware.ge.values.storable.StreamingPropertyValue;
import com.mware.ge.values.storable.Values;
import okhttp3.OkHttpClient;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import static io.bigconnect.dw.text.common.TextPropertyHelper.getTextPropertyForLanguage;
import static io.bigconnect.dw.text.summary.SummarySchemaContribution.SUMMARY;

@Name("Text summarization for Romanian")
@Description("Text summarization for Romanian text")
public class TextSummaryDetectorWorker extends DataWorker {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(TextSummaryDetectorWorker.class);
    public static final String CONFIG_API_KEY = "vllm.api.key";
    public static final String CONFIG_TIMEOUT = "ocr.timeout.seconds";
    private static final int DEFAULT_TIMEOUT_SECONDS = 30;

    public static final String CONFIG_URL = "vllm.url";

    SummaryService service;

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
        Property textProperty = BcSchema.TEXT.getFirstProperty(data.getElement());
        if (textProperty == null) {
            LOGGER.warn("text property is null");
            return;
        }

        StreamingPropertyValue spv = BcSchema.TEXT.getPropertyValue(textProperty);
        if (spv == null) {
            LOGGER.warn("text property value is null");
            return;
        }

        String text = IOUtils.toString(spv.getInputStream(), StandardCharsets.UTF_8);
        if (StringUtils.isEmpty(text)) {
            return;
        }

        try {
            String language = RawObjectSchema.RAW_LANGUAGE.getFirstPropertyValue(data.getElement());
            TextAnalysisRequest request = new TextAnalysisRequest(text, "summary", language);
            Response<TextAnalysisResponse> response = service.processText(request).execute();

            if (response.isSuccessful() && response.body() != null && response.body().summary != null) {
                ElementMutation<Vertex> m = refresh(data.getElement()).prepareMutation();
                // Use the same visibility as the text property

                String value = response.body().summary;
                SUMMARY.setProperty(data.getElement(), value, Visibility.EMPTY, getAuthorizations());

                m.save(getAuthorizations());
                getGraph().flush();
            }
        } catch (IOException e) {
            LOGGER.warn("Could not extract text summary: %s", e.getMessage());
        }
    }
}