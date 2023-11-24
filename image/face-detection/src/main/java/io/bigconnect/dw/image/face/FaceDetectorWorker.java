package io.bigconnect.dw.image.face;

import com.mware.core.ingest.dataworker.DataWorker;
import com.mware.core.ingest.dataworker.DataWorkerData;
import com.mware.core.ingest.dataworker.DataWorkerPrepareData;
import com.mware.core.model.Description;
import com.mware.core.model.Name;
import com.mware.core.model.properties.ArtifactDetectedObject;
import com.mware.core.model.properties.BcSchema;
import com.mware.core.model.properties.MediaBcSchema;
import com.mware.core.model.schema.SchemaConstants;
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
import java.util.Objects;

@Name("Face Detector")
@Description("Detects faces in images")
public class FaceDetectorWorker extends DataWorker {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(FaceDetectorWorker.class);
    public static final String CONFIG_URL = "face-detector.url";
    FaceDetectorService service;

    @Override
    public void prepare(DataWorkerPrepareData workerPrepareData) throws Exception {
        super.prepare(workerPrepareData);
        String url = getConfiguration().get(CONFIG_URL, null);
        Preconditions.checkState(!StringUtils.isEmpty(url), "Please provide the '" + CONFIG_URL + "' config parameter");
        Retrofit retrofit = new Retrofit.Builder()
                .baseUrl(url)
                .addConverterFactory(JacksonConverterFactory.create())
                .build();

        service = retrofit.create(FaceDetectorService.class);
    }

    @Override
    public boolean isHandled(Element element, Property property) {
        if (property == null) {
            return false;
        }

        // trigger manually from the image actions explorer plugin
        if (FaceDetectorSchemaContribution.DETECT_FACES.getPropertyName().equals(property.getName())) {
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
        MediaBcSchema.DETECTED_OBJECT.getProperties(element).forEach(p -> {
            MediaBcSchema.DETECTED_OBJECT.removeProperty(m, p.getKey(), p.getVisibility());
        });
        element = m.save(getAuthorizations());
        getGraph().flush();

        byte[] imageData = IOUtils.toBytes(spv.getInputStream());
        RequestBody image = RequestBody.create(MediaType.parse("image"), imageData);
        Response<FaceDetectorResponse> response = service.process(image).execute();
        FaceDetectorResponse result = response.body();
        if (result != null) {
            ElementMutation<Vertex> m2 = element.prepareMutation();

            for (FaceDetectorResponse.FaceDetectorFace item : result.faces) {
                int hash = Objects.hash(item.box.x1, item.box.y1, item.box.x2, item.box.y2, item.score);
                ArtifactDetectedObject artifact = new ArtifactDetectedObject(
                        item.box.x1, item.box.y1, item.box.x2, item.box.y2, SchemaConstants.CONCEPT_TYPE_PERSON, "Face Detection"
                );
                MediaBcSchema.DETECTED_OBJECT.addPropertyValue(m2,  String.valueOf(hash), artifact, Visibility.EMPTY);
            }
            m2.save(getAuthorizations());
            getGraph().flush();
        }
    }
}
