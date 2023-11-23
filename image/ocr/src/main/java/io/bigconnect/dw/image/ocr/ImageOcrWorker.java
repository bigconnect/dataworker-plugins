package io.bigconnect.dw.image.ocr;

import com.mware.core.ingest.dataworker.DataWorker;
import com.mware.core.ingest.dataworker.DataWorkerData;
import com.mware.core.ingest.dataworker.DataWorkerPrepareData;
import com.mware.core.ingest.dataworker.ElementOrPropertyStatus;
import com.mware.core.model.Description;
import com.mware.core.model.Name;
import com.mware.core.model.properties.BcSchema;
import com.mware.core.model.workQueue.Priority;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.ge.Element;
import com.mware.ge.Property;
import com.mware.ge.Vertex;
import com.mware.ge.mutation.ElementMutation;
import com.mware.ge.util.IOUtils;
import com.mware.ge.util.Preconditions;
import com.mware.ge.values.storable.DefaultStreamingPropertyValue;
import com.mware.ge.values.storable.StreamingPropertyValue;
import okhttp3.MediaType;
import okhttp3.MultipartBody;
import okhttp3.RequestBody;
import org.apache.commons.lang3.StringUtils;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;

import java.io.InputStream;

@Name("OCR")
@Description("Extract text from images")
public class ImageOcrWorker extends DataWorker {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(ImageOcrWorker.class);

    public static final String CONFIG_URL = "ocr.url";

    ImageOcrService service;

    @Override
    public void prepare(DataWorkerPrepareData workerPrepareData) throws Exception {
        super.prepare(workerPrepareData);
        String url = getConfiguration().get(CONFIG_URL, null);
        Preconditions.checkState(!StringUtils.isEmpty(url), "Please provide the '" + CONFIG_URL + "' config parameter");
        Retrofit retrofit = new Retrofit.Builder()
                .baseUrl(url)
                .addConverterFactory(JacksonConverterFactory.create())
                .build();

        service = retrofit.create(ImageOcrService.class);
    }

    @Override
    public boolean isHandled(Element element, Property property) {
        if (property == null) {
            return false;
        }

        // trigger manually from the image actions explorer plugin
        if (ImageOcrSchemaContribution.PERFORM_OCR.getPropertyName().equals(property.getName())) {
            String mimeType = BcSchema.MIME_TYPE.getFirstPropertyValue(element);
            return mimeType != null && mimeType.startsWith("image");
        }

        return false;
    }

    @Override
    public void execute(InputStream in, DataWorkerData data) throws Exception {
        StreamingPropertyValue spv = BcSchema.RAW.getPropertyValue(data.getElement());
        if (spv == null) {
            LOGGER.warn("Could not image data property");
            return;
        }

        // remove existing TEXT properties
        Element element = refresh(data.getElement());
        final ElementMutation<Vertex> m = element.prepareMutation();
        BcSchema.TEXT.getProperties(element).forEach(p -> {
            BcSchema.TEXT.removeProperty(m, p.getKey(), p.getVisibility());
        });
        element = m.save(getAuthorizations());
        getGraph().flush();

        byte[] imageData = IOUtils.toBytes(spv.getInputStream());
        MultipartBody.Part filePart = MultipartBody.Part.createFormData(
                "content", "file",
                RequestBody.create(MediaType.parse("image/*"), imageData)
        );
        Response<ImageOcrResponse> response = service.process(filePart).execute();
        ImageOcrResponse result = response.body();
        if (result != null) {
            ElementMutation<Vertex> m2 = element.prepareMutation();
            String text = StringUtils.trimToEmpty(result.text);
            String propKey = ""+System.currentTimeMillis();
            BcSchema.TEXT.addPropertyValue(m2, propKey, DefaultStreamingPropertyValue.create(text), data.getVisibility());
            element = m2.save(getAuthorizations());
            getGraph().flush();

            getWorkQueueRepository().pushOnDwQueue(
                    element,
                    propKey,
                    BcSchema.TEXT.getPropertyName(),
                    data.getWorkspaceId(),
                    data.getVisibilitySource(),
                    Priority.HIGH,
                    ElementOrPropertyStatus.UPDATE,
                    null
            );
            pushTextUpdated(data);
        }
    }
}
