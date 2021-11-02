package io.bigconnect.dw.google.vision;

import com.google.cloud.vision.v1.NormalizedVertex;
import com.mware.bigconnect.image.ImageTagger;
import com.mware.core.InMemoryGraphTestBase;
import com.mware.core.ingest.dataworker.DataWorkerData;
import com.mware.core.ingest.dataworker.DataWorkerPrepareData;
import com.mware.core.model.properties.BcSchema;
import com.mware.core.model.schema.SchemaConstants;
import com.mware.core.model.termMention.TermMentionRepository;
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
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ObjectDetectorDataWorkerTest extends InMemoryGraphTestBase {
    Authorizations AUTHS = new Authorizations();

    @Test
    public void testDetectObjects() throws Exception {
        GoogleCredentialUtils.checkCredentials();

        InputStream videoFile = getClass().getResourceAsStream("/test.jpeg");
        byte[] bytes = IOUtils.toByteArray(videoFile);

        Metadata mimeTypeMetadata = Metadata.create();
        BcSchema.MIME_TYPE_METADATA.setMetadata(mimeTypeMetadata, "image/jpeg", Visibility.EMPTY);

        Vertex v = getGraph().prepareVertex(Visibility.EMPTY, SchemaConstants.CONCEPT_TYPE_IMAGE)
                .setProperty(BcSchema.RAW.getPropertyName(), new DefaultStreamingPropertyValue(new ByteArrayInputStream(bytes), ByteArray.class), mimeTypeMetadata, Visibility.EMPTY)
                .save(AUTHS);
        videoFile.close();

        ObjectDetectorPostMimeTypeWorker dw = new ObjectDetectorPostMimeTypeWorker();
        dw.setConfiguration(getConfiguration());
        dw.setGraph(getGraph());
        dw.setSchemaRepository(getSchemaRepository());
        dw.setWorkQueueRepository(getWorkQueueRepository());
        dw.setWebQueueRepository(getWebQueueRepository());
        dw.setGraphAuthorizationRepository(getGraphAuthorizationRepository());
        dw.prepare(new DataWorkerPrepareData(getConfigurationMap(), Collections.emptyList(), getUser(), AUTHS, null));

        DataWorkerData data = new DataWorkerData(new DirectVisibilityTranslator(), v, BcSchema.RAW.getProperty(v),
                null, null, Priority.NORMAL, true);
        dw.execute("image/jpegn", data, AUTHS);
    }

    @Test
    public void testImageTagger() throws IOException {
        ImageTagger imageTagger = new ImageTagger(getConfiguration(), getGraph(), getSchemaRepository(), AUTHS,
                getWorkQueueRepository(), getWebQueueRepository(),
                new TermMentionRepository(getGraph(), getGraphAuthorizationRepository()));

        NormalizedVertex v1 = NormalizedVertex.newBuilder().setX(0.31451407f).setY(0.78488755f).build();
        NormalizedVertex v2 = NormalizedVertex.newBuilder().setX(0.44057098f).setY(0.78488755f).build();
        NormalizedVertex v3 = NormalizedVertex.newBuilder().setX(0.44057098f).setY(0.97062904f).build();
        NormalizedVertex v4 = NormalizedVertex.newBuilder().setX(0.31451407f).setY(0.97062904f).build();
        List<NormalizedVertex> box = Arrays.asList(v1, v2, v3, v4);
        InputStream videoFile = getClass().getResourceAsStream("/test.jpeg");
        byte[] bytes = IOUtils.toByteArray(videoFile);

        Vertex v = getGraph().prepareVertex(Visibility.EMPTY, SchemaConstants.CONCEPT_TYPE_IMAGE)
                .setProperty(BcSchema.RAW.getPropertyName(), new DefaultStreamingPropertyValue(new ByteArrayInputStream(bytes), ByteArray.class), Visibility.EMPTY)
                .save(AUTHS);
        videoFile.close();

        imageTagger.createObject(v, box.get(0).getX(), box.get(0).getY(), box.get(2).getX(), box.get(2).getY(), 0.1f, "tag1");

        Assert.assertEquals(1, 1);
    }
}
