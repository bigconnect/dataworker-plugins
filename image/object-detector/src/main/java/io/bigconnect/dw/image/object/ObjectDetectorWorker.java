package io.bigconnect.dw.image.object;

import com.mware.core.ingest.dataworker.DataWorker;
import com.mware.core.ingest.dataworker.DataWorkerData;
import com.mware.core.ingest.dataworker.DataWorkerPrepareData;
import com.mware.core.model.Description;
import com.mware.core.model.Name;
import com.mware.core.model.properties.ArtifactDetectedObject;
import com.mware.core.model.properties.BcSchema;
import com.mware.core.model.properties.MediaBcSchema;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.ge.*;
import com.mware.ge.mutation.ElementMutation;
import com.mware.ge.util.IOUtils;
import com.mware.ge.util.Preconditions;
import com.mware.ge.values.storable.StreamingPropertyValue;
import com.mware.ge.values.storable.Values;
import okhttp3.MediaType;
import okhttp3.RequestBody;
import org.apache.commons.lang3.StringUtils;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;

import java.io.InputStream;
import java.util.*;

@Name("Object detector")
@Description("Object detector from images")
public class ObjectDetectorWorker extends DataWorker {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(ObjectDetectorWorker.class);

    public static final String CONFIG_URL = "object-detector.url";
    ObjectDetectorService service;

    @Override
    public void prepare(DataWorkerPrepareData workerPrepareData) throws Exception {
        super.prepare(workerPrepareData);
        String url = getConfiguration().get(CONFIG_URL, null);
        Preconditions.checkState(!StringUtils.isEmpty(url), "Please provide the '" + CONFIG_URL + "' config parameter");
        Retrofit retrofit = new Retrofit.Builder()
                .baseUrl(url)
                .addConverterFactory(JacksonConverterFactory.create())
                .build();

        service = retrofit.create(ObjectDetectorService.class);
    }

    @Override
    public boolean isHandled(Element element, Property property) {
        if (property == null) {
            return false;
        }

        // trigger manually from the image actions explorer plugin
        if (ObjectDetectorSchemaContribution.DETECT_OBJECTS.getPropertyName().equals(property.getName())) {
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
        // delete existing detected objects
        MediaBcSchema.IMAGE_TAG.getProperties(element).forEach(p -> {
            MediaBcSchema.IMAGE_TAG.removeProperty(m, p.getKey(), p.getVisibility());
        });
        element = m.save(getAuthorizations());
        getGraph().flush();

        byte[] imageData = IOUtils.toBytes(spv.getInputStream());
        RequestBody image = RequestBody.create(MediaType.parse("image"), imageData);
        Response<ObjectDetectorResponse> response = service.process(image).execute();
        ObjectDetectorResponse result = response.body();
        if (result != null) {
            ElementMutation<Vertex> m2 = element.prepareMutation();
            Set<String> labels = new HashSet<>();
            for (ObjectDetectorResponse.ObjectDetectorItem item : result.objects) {
                if (labels.contains(item.label)) {
                    continue;
                }
                int hash = Objects.hash(item.box.bottom_right_x, item.box.bottom_right_y, item.box.top_left_x, item.box.top_left_y, item.label, item.score);
                Metadata propMetadata = Metadata.create();
                MediaBcSchema.IMAGE_TAG_SCORE.setMetadata(propMetadata, item.score, Visibility.EMPTY);
                MediaBcSchema.IMAGE_TAG.addPropertyValue(m2, String.valueOf(hash), item.label, propMetadata, Visibility.EMPTY);
                labels.add(item.label);
            }
            m2.save(getAuthorizations());
            getGraph().flush();
        }
    }
}
