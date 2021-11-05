package io.bigconnect.dw.google.speech;

import com.mware.bigconnect.ffmpeg.AudioFormat;
import com.mware.bigconnect.ffmpeg.VideoFormat;
import com.mware.core.InMemoryGraphTestBase;
import com.mware.core.model.longRunningProcess.LongRunningWorkerPrepareData;
import com.mware.core.model.properties.BcSchema;
import com.mware.core.model.properties.MediaBcSchema;
import com.mware.core.model.properties.RawObjectSchema;
import com.mware.core.model.schema.SchemaConstants;
import com.mware.core.util.ClientApiConverter;
import com.mware.ge.Authorizations;
import com.mware.ge.Vertex;
import com.mware.ge.Visibility;
import com.mware.ge.values.storable.ByteArray;
import com.mware.ge.values.storable.DefaultStreamingPropertyValue;
import com.mware.ge.values.storable.Values;
import io.bigconnect.dw.google.common.schema.GoogleCredentialUtils;
import org.apache.commons.io.IOUtils;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.InputStream;

import static io.bigconnect.dw.google.speech.Speech2TextSchemaContribution.GOOGLE_S2T_DONE_PROPERTY;
import static io.bigconnect.dw.google.speech.Speech2TextSchemaContribution.GOOGLE_S2T_PROGRESS_PROPERTY;

public class Speech2TextDataWorkerTest extends InMemoryGraphTestBase {
    Authorizations AUTHS = new Authorizations();

    @Test
    public void testSpeech2Text() throws Exception {
        GoogleCredentialUtils.checkCredentials();

        getConfiguration().set("google.s2t.bucket.name", "ums2t");

        InputStream videoFile = new FileInputStream("/home/flavius/Downloads/test.mp4");
        byte[] bytes = IOUtils.toByteArray(videoFile);
        Vertex v = getGraph().prepareVertex(Visibility.EMPTY, SchemaConstants.CONCEPT_TYPE_VIDEO)
                .setProperty(GOOGLE_S2T_PROGRESS_PROPERTY.getPropertyName(), Values.booleanValue(false), Visibility.EMPTY)
                .setProperty(RawObjectSchema.RAW_LANGUAGE.getPropertyName(), Values.stringValue("ro"), Visibility.EMPTY)
                .setProperty(MediaBcSchema.MEDIA_VIDEO_FORMAT.getPropertyName(), Values.stringValue(VideoFormat.MP4.name()), Visibility.EMPTY)
                .setProperty(MediaBcSchema.MEDIA_AUDIO_FORMAT.getPropertyName(), Values.stringValue(AudioFormat.MP4.name()), Visibility.EMPTY)
                .setProperty(BcSchema.RAW.getPropertyName(), new DefaultStreamingPropertyValue(new ByteArrayInputStream(bytes), ByteArray.class), Visibility.EMPTY)
                .save(AUTHS);
        videoFile.close();

        Speech2TextLongRunningProcessWorker lrp = new Speech2TextLongRunningProcessWorker(
                getLongRunningProcessRepository(), getGraph(), getConfiguration(), getSchemaRepository(), getWorkQueueRepository(), getWebQueueRepository()
        );
        lrp.setMetricRegistry(graph);
        lrp.prepare(new LongRunningWorkerPrepareData(getConfigurationMap(), getUser(), null));
        JSONObject jsonObject = ClientApiConverter.clientApiToJSONObject(
                new Speech2TextQueueItem(getUser().getUserId(), null, AUTHS.getAuthorizations(), v.getId())
        );
        jsonObject.put("id", "111");
        lrp.processInternal(jsonObject);

        Assert.assertTrue(GOOGLE_S2T_DONE_PROPERTY.getPropertyValue(v, false));
    }
}
