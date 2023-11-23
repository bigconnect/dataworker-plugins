package io.bigconnect.dw.image;

import com.mware.core.InMemoryGraphTestBase;
import com.mware.core.ingest.dataworker.DataWorkerData;
import com.mware.core.ingest.dataworker.DataWorkerPrepareData;
import com.mware.core.model.properties.ArtifactDetectedObject;
import com.mware.core.model.properties.BcSchema;
import com.mware.core.model.properties.MediaBcSchema;
import com.mware.core.model.properties.RawObjectSchema;
import com.mware.core.model.schema.SchemaConstants;
import com.mware.core.model.workQueue.Priority;
import com.mware.core.security.DirectVisibilityTranslator;
import com.mware.ge.Authorizations;
import com.mware.ge.Property;
import com.mware.ge.Vertex;
import com.mware.ge.Visibility;
import com.mware.ge.values.storable.ByteArray;
import com.mware.ge.values.storable.DefaultStreamingPropertyValue;
import com.mware.ge.values.storable.Values;
import io.bigconnect.dw.image.object.ObjectDetectorSchemaContribution;
import io.bigconnect.dw.image.object.ObjectDetectorWorker;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.net.URL;
import java.util.Collections;

public class ObjectDetectorTest extends InMemoryGraphTestBase {
    Authorizations AUTHS = new Authorizations();

    @Test
    public void testObjectDetection() throws Exception {
        getConfiguration().set(ObjectDetectorWorker.CONFIG_URL, String.format("http://%s:%d", "localhost", 9989));

        ObjectDetectorWorker dw = new ObjectDetectorWorker();
        dw.setConfiguration(getConfiguration());
        dw.setGraph(getGraph());
        dw.setWorkQueueRepository(getWorkQueueRepository());
        dw.setWebQueueRepository(getWebQueueRepository());
        dw.setVisibilityTranslator(new DirectVisibilityTranslator());
        dw.prepare(new DataWorkerPrepareData(getConfigurationMap(), Collections.emptyList(), getUser(), AUTHS, null));

        byte[] imageData = IOUtils.toByteArray(new URL("https://farm8.staticflickr.com/7451/8997688297_f5276c4bd2_z.jpg"));
        Vertex v = getGraph().prepareVertex(Visibility.EMPTY, SchemaConstants.CONCEPT_TYPE_IMAGE)
                .setProperty(BcSchema.RAW.getPropertyName(), new DefaultStreamingPropertyValue(new ByteArrayInputStream(imageData), ByteArray.class), Visibility.EMPTY)
                .setProperty(BcSchema.MIME_TYPE.getPropertyName(), Values.stringValue("image/png"), Visibility.EMPTY)
                .setProperty(ObjectDetectorSchemaContribution.DETECT_OBJECTS.getPropertyName(), Values.booleanValue(true), Visibility.EMPTY)
                .save(AUTHS);

        Assert.assertTrue(dw.isHandled(v, ObjectDetectorSchemaContribution.DETECT_OBJECTS.getProperty(v)));
        DataWorkerData data = new DataWorkerData(new DirectVisibilityTranslator(), v, RawObjectSchema.RAW_LANGUAGE.getFirstProperty(v),
                null, null, Priority.NORMAL, true);
        dw.execute(new ByteArrayInputStream(imageData), data);
        v = getGraph().getVertex(v.getId(), AUTHS);

        for (Property property : MediaBcSchema.IMAGE_TAG.getProperties(v)) {
            String detectedObject = MediaBcSchema.IMAGE_TAG.getPropertyValue(property);
            System.out.println(detectedObject);
        }
    }
}
