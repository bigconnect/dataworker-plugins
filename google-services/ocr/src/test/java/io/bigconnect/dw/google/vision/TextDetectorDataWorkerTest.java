package io.bigconnect.dw.google.vision;

import com.mware.core.InMemoryGraphTestBase;
import com.mware.core.ingest.dataworker.DataWorkerData;
import com.mware.core.ingest.dataworker.DataWorkerPrepareData;
import com.mware.core.model.properties.BcSchema;
import com.mware.core.model.schema.SchemaConstants;
import com.mware.core.model.workQueue.Priority;
import com.mware.core.security.DirectVisibilityTranslator;
import com.mware.ge.Authorizations;
import com.mware.ge.Metadata;
import com.mware.ge.Vertex;
import com.mware.ge.Visibility;
import com.mware.ge.values.storable.ByteArray;
import com.mware.ge.values.storable.DefaultStreamingPropertyValue;
import io.bigconnect.dw.google.common.schema.GoogleCredentialUtils;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.util.Collections;

public class TextDetectorDataWorkerTest extends InMemoryGraphTestBase {
    Authorizations AUTHS = new Authorizations();

    @Test
    public void testOcr() throws Exception {
        GoogleCredentialUtils.checkCredentials();

        InputStream videoFile = getClass().getResourceAsStream("/test.jpg");
        byte[] bytes = IOUtils.toByteArray(videoFile);

        Metadata mimeTypeMetadata = Metadata.create();
        BcSchema.MIME_TYPE_METADATA.setMetadata(mimeTypeMetadata, "image/jpeg", Visibility.EMPTY);

        Vertex v = getGraph().prepareVertex(Visibility.EMPTY, SchemaConstants.CONCEPT_TYPE_IMAGE)
                .setProperty(BcSchema.RAW.getPropertyName(), new DefaultStreamingPropertyValue(new ByteArrayInputStream(bytes), ByteArray.class), mimeTypeMetadata, Visibility.EMPTY)
                .save(AUTHS);
        videoFile.close();

        TextDetectorPostMimeTypeWorker dw = new TextDetectorPostMimeTypeWorker();
        dw.setGraph(getGraph());
        dw.prepare(new DataWorkerPrepareData(getConfigurationMap(), Collections.emptyList(), getUser(), AUTHS, null));
        dw.setWorkQueueRepository(getWorkQueueRepository());
        dw.setWebQueueRepository(getWebQueueRepository());

        DataWorkerData data = new DataWorkerData(new DirectVisibilityTranslator(), v, BcSchema.RAW.getProperty(v),
                null, null, Priority.NORMAL, true);
        dw.execute("image/jpeg", data, AUTHS);

        v = getGraph().getVertex(v.getId(), AUTHS);
        String text = BcSchema.TEXT.getFirstPropertyValueAsString(v);
        Assert.assertEquals("WAITING?\n" +
                "PLEASE\n" +
                "TURN OFF\n" +
                "YOUR\n" +
                "ENGINE\n" +
                " WAITING? PLEASE TURN OFF YOUR ENGINE ", text);
    }
}
