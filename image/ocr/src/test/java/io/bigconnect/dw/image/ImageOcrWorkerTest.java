package io.bigconnect.dw.image;

import com.mware.core.InMemoryGraphTestBase;
import com.mware.core.ingest.dataworker.DataWorkerData;
import com.mware.core.ingest.dataworker.DataWorkerPrepareData;
import com.mware.core.model.properties.BcSchema;
import com.mware.core.model.properties.RawObjectSchema;
import com.mware.core.model.schema.SchemaConstants;
import com.mware.core.model.workQueue.Priority;
import com.mware.core.security.DirectVisibilityTranslator;
import com.mware.ge.Authorizations;
import com.mware.ge.Vertex;
import com.mware.ge.Visibility;
import com.mware.ge.values.storable.ByteArray;
import com.mware.ge.values.storable.DefaultStreamingPropertyValue;
import com.mware.ge.values.storable.StreamingPropertyValue;
import com.mware.ge.values.storable.Values;
import io.bigconnect.dw.image.ocr.ImageOcrSchemaContribution;
import io.bigconnect.dw.image.ocr.ImageOcrWorker;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.net.URL;
import java.util.Collections;

public class ImageOcrWorkerTest extends InMemoryGraphTestBase {
    Authorizations AUTHS = new Authorizations();

    @Test
    public void testObjectDetection() throws Exception {
        getConfiguration().set(ImageOcrWorker.CONFIG_URL, String.format("http://%s:%d", "localhost", 8989));

        ImageOcrWorker dw = new ImageOcrWorker();
        dw.setConfiguration(getConfiguration());
        dw.setGraph(getGraph());
        dw.setWorkQueueRepository(getWorkQueueRepository());
        dw.setWebQueueRepository(getWebQueueRepository());
        dw.setVisibilityTranslator(new DirectVisibilityTranslator());
        dw.prepare(new DataWorkerPrepareData(getConfigurationMap(), Collections.emptyList(), getUser(), AUTHS, null));

        byte[] imageData = IOUtils.toByteArray(new URL("https://assets-global.website-files.com/5ebb0930dd82631397ddca92/61bb9a7943343e03bb9fcd1b_documents-product-template-software.png"));
        Vertex v = getGraph().prepareVertex(Visibility.EMPTY, SchemaConstants.CONCEPT_TYPE_IMAGE)
                .setProperty(BcSchema.RAW.getPropertyName(), new DefaultStreamingPropertyValue(new ByteArrayInputStream(imageData), ByteArray.class), Visibility.EMPTY)
                .setProperty(BcSchema.MIME_TYPE.getPropertyName(), Values.stringValue("image/png"), Visibility.EMPTY)
                .setProperty(ImageOcrSchemaContribution.PERFORM_OCR.getPropertyName(), Values.booleanValue(true), Visibility.EMPTY)
                .save(AUTHS);

        Assert.assertTrue(dw.isHandled(v, ImageOcrSchemaContribution.PERFORM_OCR.getProperty(v)));
        DataWorkerData data = new DataWorkerData(new DirectVisibilityTranslator(), v, RawObjectSchema.RAW_LANGUAGE.getFirstProperty(v),
                null, null, Priority.NORMAL, true);
        dw.execute(new ByteArrayInputStream(imageData), data);
        v = getGraph().getVertex(v.getId(), AUTHS);

        StreamingPropertyValue spv = BcSchema.TEXT.getFirstPropertyValue(v);
        Assert.assertNotNull(spv);
        System.out.println(spv.readToString());
    }
}
