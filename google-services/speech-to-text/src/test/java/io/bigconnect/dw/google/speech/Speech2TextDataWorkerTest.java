package io.bigconnect.dw.google.speech;

import com.mware.bigconnect.ffmpeg.AudioFormat;
import com.mware.bigconnect.ffmpeg.VideoFormat;
import com.mware.core.InMemoryGraphTestBase;
import com.mware.core.ingest.dataworker.DataWorkerData;
import com.mware.core.ingest.dataworker.DataWorkerPrepareData;
import com.mware.core.model.properties.BcSchema;
import com.mware.core.model.properties.MediaBcSchema;
import com.mware.core.model.properties.RawObjectSchema;
import com.mware.core.model.schema.SchemaConstants;
import com.mware.core.model.workQueue.Priority;
import com.mware.core.security.DirectVisibilityTranslator;
import com.mware.ge.Authorizations;
import com.mware.ge.Vertex;
import com.mware.ge.Visibility;
import com.mware.ge.values.storable.ByteArray;
import com.mware.ge.values.storable.DefaultStreamingPropertyValue;
import com.mware.ge.values.storable.Values;
import io.bigconnect.dw.google.common.schema.GoogleCredentialUtils;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Collections;

import static io.bigconnect.dw.google.speech.Speech2TextSchemaContribution.*;

public class Speech2TextDataWorkerTest extends InMemoryGraphTestBase {
    Authorizations AUTHS = new Authorizations();

    @Test
    public void testSpeech2Text() throws Exception {
        GoogleCredentialUtils.checkCredentials();

        getConfiguration().set("google.s2t.bucket.name", "ums2t");
        Speech2TextOperationMonitorService monitorService =
                new Speech2TextOperationMonitorService(getLockRepository(), getWorkQueueRepository(), getWebQueueRepository(),
                        getGraph(), getConfiguration(), getLifeSupportService());

        InputStream videoFile = getClass().getResourceAsStream("/test.mp4");
        byte[] bytes = IOUtils.toByteArray(videoFile);
        Vertex v = getGraph().prepareVertex(Visibility.EMPTY, SchemaConstants.CONCEPT_TYPE_VIDEO)
                .setProperty(GOOGLE_S2T_PROPERTY.getPropertyName(), Values.booleanValue(true), Visibility.EMPTY)
                .setProperty(GOOGLE_S2T_PROGRESS_PROPERTY.getPropertyName(), Values.booleanValue(false), Visibility.EMPTY)
                .setProperty(RawObjectSchema.RAW_LANGUAGE.getPropertyName(), Values.stringValue("ro"), Visibility.EMPTY)
                .setProperty(MediaBcSchema.MEDIA_VIDEO_FORMAT.getPropertyName(), Values.stringValue(VideoFormat.MP4.name()), Visibility.EMPTY)
                .setProperty(MediaBcSchema.MEDIA_AUDIO_FORMAT.getPropertyName(), Values.stringValue(AudioFormat.MP4.name()), Visibility.EMPTY)
                .setProperty(BcSchema.RAW.getPropertyName(), new DefaultStreamingPropertyValue(new ByteArrayInputStream(bytes), ByteArray.class), Visibility.EMPTY)
                .save(AUTHS);
        videoFile.close();


        SpeechToTextDataWorker dw = new SpeechToTextDataWorker(getConfiguration(), monitorService);
        dw.setGraph(getGraph());
        dw.prepare(new DataWorkerPrepareData(getConfigurationMap(), Collections.emptyList(), getUser(), AUTHS, null));

        Assert.assertTrue(dw.isHandled(v, GOOGLE_S2T_PROPERTY.getProperty(v)));

        DataWorkerData data = new DataWorkerData(new DirectVisibilityTranslator(), v, GOOGLE_S2T_PROPERTY.getProperty(v),
                null, null, Priority.NORMAL, true);
        dw.execute(new ByteArrayInputStream(bytes), data);

        int counter = 0;
        while (!GOOGLE_S2T_DONE_PROPERTY.getPropertyValue(v, false)) {
            monitorService.run();
            counter++;
            Thread.sleep(2000);
            if (counter >= 10000) {
                break;
            }
        }

        Assert.assertTrue(GOOGLE_S2T_DONE_PROPERTY.getPropertyValue(v, false));
    }

    @Test
    public void testUtf8StringCleaning() {
        String str = "\u0012u\nl\netoate tipurile de vaccin România menține aceeași opinie campania de vaccinare trebuie să continue\u0015�\b_?*\u0005ro-ro\u001A\u0002\b\u000F";

    }
}
