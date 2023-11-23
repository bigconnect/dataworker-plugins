package io.bigconnect.dw.image.object;

import com.mware.core.ingest.dataworker.DataWorker;
import com.mware.core.ingest.dataworker.DataWorkerData;
import com.mware.core.ingest.dataworker.DataWorkerPrepareData;
import com.mware.core.model.Description;
import com.mware.core.model.Name;
import com.mware.core.model.properties.ArtifactDetectedObject;
import com.mware.core.model.properties.BcSchema;
import com.mware.core.model.properties.MediaBcSchema;
import com.mware.core.model.workQueue.Priority;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.ge.Element;
import com.mware.ge.Metadata;
import com.mware.ge.Property;
import com.mware.ge.Vertex;
import com.mware.ge.util.IOUtils;
import com.mware.ge.util.Preconditions;
import com.mware.ge.values.storable.StreamingPropertyValue;
import org.apache.commons.lang3.StringUtils;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

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

        String mimeType = BcSchema.MIME_TYPE.getFirstPropertyValue(element);
        return mimeType != null && mimeType.startsWith("image");
    }

    @Override
    public void execute(InputStream in, DataWorkerData data) throws Exception {
        StreamingPropertyValue spv = BcSchema.RAW.getPropertyValue(data.getElement());
        if (spv == null) {
            LOGGER.warn("Could not image data property");
            return;
        }
        byte[] imageData = IOUtils.toBytes(spv.getInputStream());
        Response<ObjectDetectorResponse> response = service.process(imageData).execute();
        ObjectDetectorResponse result = response.body();
        if (result != null) {
            Integer imageWidth = MediaBcSchema.MEDIA_WIDTH.getPropertyValue(data.getElement());
            Integer imageHeight = MediaBcSchema.MEDIA_HEIGHT.getPropertyValue(data.getElement());
            List<ArtifactDetectedObject> detectedObjects = new ArrayList<>();
            for (ObjectDetectorResponse.ObjectDetectorItem item : result.objects) {
                detectedObjects.add(
                        new ArtifactDetectedObject(
                                item.box.top_left_x / imageWidth,
                                item.box.top_left_y / imageHeight,
                                item.box.bottom_right_x / imageWidth,
                                item.box.bottom_right_y / imageHeight,
                                "", ""
                        )
                );
            }

            Metadata metadata = data.createPropertyMetadata(getUser());
            saveDetectedObjects((Vertex) data.getElement(), metadata, detectedObjects, Priority.HIGH);
        }
    }

    private void saveDetectedObjects(Vertex artifactVertex, Metadata metadata, List<ArtifactDetectedObject> detectedObjects, Priority priority) {
        for (ArtifactDetectedObject detectedObject : detectedObjects) {
            saveDetectedObject(artifactVertex, metadata, detectedObject);
        }
        getGraph().flush();
    }

    private String saveDetectedObject(Vertex artifactVertex, Metadata metadata, ArtifactDetectedObject detectedObject) {
        String multiKey = detectedObject.getMultivalueKey(ObjectDetectorWorker.class.getSimpleName());
        MediaBcSchema.DETECTED_OBJECT.addPropertyValue(artifactVertex, multiKey, detectedObject, metadata, artifactVertex.getVisibility(), getAuthorizations());
        return multiKey;
    }
}
