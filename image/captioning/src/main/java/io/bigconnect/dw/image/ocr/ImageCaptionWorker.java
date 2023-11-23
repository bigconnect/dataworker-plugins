package io.bigconnect.dw.image.ocr;

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
import com.mware.ge.util.IOUtils;
import com.mware.ge.util.Preconditions;
import com.mware.ge.values.storable.StreamingPropertyValue;
import okhttp3.MediaType;
import okhttp3.RequestBody;
import org.apache.commons.lang3.StringUtils;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;

import java.io.InputStream;

@Name("Image Captioning")
@Description("Describe an image")
public class ImageCaptionWorker extends DataWorker {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(ImageCaptionWorker.class);

    public static final String CONFIG_URL = "captioning.url";

    ImageCaptioningService service;

    @Override
    public void prepare(DataWorkerPrepareData workerPrepareData) throws Exception {
        super.prepare(workerPrepareData);

        String url = getConfiguration().get(CONFIG_URL, null);
        Preconditions.checkState(!StringUtils.isEmpty(url), "Please provide the '" + CONFIG_URL + "' config parameter");
        Retrofit retrofit = new Retrofit.Builder()
                .baseUrl(url)
                .addConverterFactory(JacksonConverterFactory.create())
                .build();

        service = retrofit.create(ImageCaptioningService.class);
    }

    @Override
    public boolean isHandled(Element element, Property property) {
        if (property == null) {
            return false;
        }

        // trigger manually from the image actions explorer plugin
        if (ImageCaptionSchemaContribution.PERFORM_CAPTION.getPropertyName().equals(property.getName())) {
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

        Element element = refresh(data.getElement());
        final ElementMutation<Vertex> m = element.prepareMutation();
        // delete existing caption
        ImageCaptionSchemaContribution.CAPTION.removeProperty(m, Visibility.EMPTY);
        element = m.save(getAuthorizations());
        getGraph().flush();

        byte[] imageData = IOUtils.toBytes(spv.getInputStream());
        RequestBody image = RequestBody.create(MediaType.parse("image"), imageData);
        Response<ImageCaptioningResponse> response = service.process(image).execute();
        ImageCaptioningResponse result = response.body();
        if (result != null) {
            ElementMutation<Vertex> m2 = element.prepareMutation();
            ImageCaptionSchemaContribution.CAPTION.setProperty(m2, StringUtils.trimToEmpty(result.text), Visibility.EMPTY);
            m2.save(getAuthorizations());
        }
    }
}
