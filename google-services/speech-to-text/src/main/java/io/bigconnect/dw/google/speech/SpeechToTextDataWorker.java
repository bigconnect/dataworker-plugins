/*
 * This file is part of the BigConnect project.
 *
 * Copyright (c) 2013-2020 MWARE SOLUTIONS SRL
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License version 3
 * as published by the Free Software Foundation with the addition of the
 * following permission added to Section 15 as permitted in Section 7(a):
 * FOR ANY PART OF THE COVERED WORK IN WHICH THE COPYRIGHT IS OWNED BY
 * MWARE SOLUTIONS SRL, MWARE SOLUTIONS SRL DISCLAIMS THE WARRANTY OF
 * NON INFRINGEMENT OF THIRD PARTY RIGHTS

 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License
 * along with this program; if not, see http://www.gnu.org/licenses or write to
 * the Free Software Foundation, Inc., 51 Franklin Street, Fifth Floor,
 * Boston, MA, 02110-1301 USA, or download the license from the following URL:
 * https://www.gnu.org/licenses/agpl-3.0.txt
 *
 * The interactive user interfaces in modified source and object code versions
 * of this program must display Appropriate Legal Notices, as required under
 * Section 5 of the GNU Affero General Public License.
 *
 * You can be released from the requirements of the license by purchasing
 * a commercial license. Buying such a license is mandatory as soon as you
 * develop commercial activities involving the BigConnect software without
 * disclosing the source code of your own applications.
 *
 * These activities include: offering paid services to customers as an ASP,
 * embedding the product in a web application, shipping BigConnect with a
 * closed source product.
 */
package io.bigconnect.dw.google.speech;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.speech.v1.*;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.inject.Inject;
import com.mware.bigconnect.ffmpeg.AVMediaInfo;
import com.mware.bigconnect.ffmpeg.AVUtils;
import com.mware.bigconnect.ffmpeg.AudioFormat;
import com.mware.bigconnect.ffmpeg.VideoFormat;
import com.mware.core.config.Configuration;
import com.mware.core.ingest.dataworker.DataWorker;
import com.mware.core.ingest.dataworker.DataWorkerData;
import com.mware.core.model.Description;
import com.mware.core.model.Name;
import com.mware.core.model.properties.BcSchema;
import com.mware.core.model.properties.MediaBcSchema;
import com.mware.core.model.properties.RawObjectSchema;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.ge.*;
import com.mware.ge.util.ArrayUtils;
import com.mware.ge.util.Preconditions;
import com.mware.ge.values.storable.StreamingPropertyValue;
import com.mware.ge.values.storable.Values;
import io.bigconnect.dw.google.common.schema.GoogleCredentialUtils;
import io.bigconnect.dw.google.common.schema.GoogleSchemaContribution;
import net.bramp.ffmpeg.FFmpegExecutor;
import net.bramp.ffmpeg.builder.FFmpegBuilder;
import net.bramp.ffmpeg.job.FFmpegJob;
import net.bramp.ffmpeg.probe.FFmpegProbeResult;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

import static io.bigconnect.dw.google.speech.Speech2TextSchemaContribution.GOOGLE_S2T_PROGRESS_PROPERTY;
import static io.bigconnect.dw.google.speech.Speech2TextSchemaContribution.GOOGLE_S2T_PROPERTY;

@Name("Google Speech2Text Data Worker")
@Description("Performs Speech2Text on Audio/Video files using Google Cloud Services")
public class SpeechToTextDataWorker extends DataWorker {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(SpeechToTextDataWorker.class);
    private static final VideoFormat[] ALLOWED_VIDEO_FORMATS = new VideoFormat[] { VideoFormat.MP4 };
    private static final AudioFormat[] ALLOWED_AUDIO_FORMATS = new AudioFormat[] { AudioFormat.MP4 };
    private final String bucketName;

    @Inject
    public SpeechToTextDataWorker(Configuration configuration, Speech2TextOperationMonitorService monitorService) {
        this.bucketName = configuration.get(Speech2TextOperationMonitorService.CONFIG_GOOGLE_S2T_BUCKET_NAME, "");
        Preconditions.checkState(!StringUtils.isEmpty(bucketName),
                "Please provide the " + Speech2TextOperationMonitorService.CONFIG_GOOGLE_S2T_BUCKET_NAME + " configuration property");
    }

    @Override
    public boolean isHandled(Element element, Property property) {
        if (property == null) {
            return false;
        }

        if (GOOGLE_S2T_PROPERTY.getPropertyName().equals(property.getName())) {
            Boolean performS2T = Speech2TextSchemaContribution.GOOGLE_S2T_PROPERTY.getPropertyValue(element, false);
            if (Boolean.TRUE.equals(performS2T)) {
                // check to see if we have an already in progress operation
                boolean alreadyInProgress = GOOGLE_S2T_PROGRESS_PROPERTY.getPropertyValue(element, false);
                if (alreadyInProgress) {
                    LOGGER.warn("Speech2Text alrady in progress for element: "+element.getId());
                    return false;
                }

                // check language and videoformat
                final String language = RawObjectSchema.RAW_LANGUAGE.getFirstPropertyValue(element);
                final String videoFormat = MediaBcSchema.MEDIA_VIDEO_FORMAT.getPropertyValue(element);
                final String audioFormat = MediaBcSchema.MEDIA_AUDIO_FORMAT.getPropertyValue(element);
                boolean hasVideo = !StringUtils.isEmpty(videoFormat) && ArrayUtils.contains(ALLOWED_VIDEO_FORMATS, VideoFormat.valueOf(videoFormat));
                boolean hasAudio = !StringUtils.isEmpty(audioFormat) && ArrayUtils.contains(ALLOWED_AUDIO_FORMATS, VideoFormat.valueOf(audioFormat));

                return !StringUtils.isEmpty(language) && (hasVideo || hasAudio);
            }
        }

        return false;
    }

    @Override
    public void execute(InputStream in, DataWorkerData data) throws Exception {
        LOGGER.debug("Executing Speech2Text Data Worker");

        Path tempFolder = Files.createTempDirectory(S2TConstants.SPEECH_TEMP_DIR_PREFIX);
        final Vertex vertex = (Vertex) data.getElement();
        final Property languageProp = RawObjectSchema.RAW_LANGUAGE.getFirstProperty(vertex);
        if (languageProp == null) {
            return;
        }
        final String language = languageProp.getValue().asObjectCopy().toString();
        if (StringUtils.isEmpty(language)) {
            return;
        }

        GOOGLE_S2T_PROPERTY.setProperty(vertex, Boolean.FALSE, Visibility.EMPTY, vertex.getAuthorizations());
        GOOGLE_S2T_PROGRESS_PROPERTY.setProperty(vertex, Boolean.TRUE, Visibility.EMPTY, vertex.getAuthorizations());
        getGraph().flush();

        // Convert to FLAC audio
        Path audioPath = createFlac(vertex, tempFolder);
        // Upload to GCS
        final String gcsId = UUID.randomUUID().toString();
        uploadObjectToGCS(gcsId, audioPath.toAbsolutePath().toString());
        // Probe Audio info
        final AudioInfo audioInfo = new AudioInfo(audioPath.toAbsolutePath().toString());
        // Cleanup
        FileUtils.deleteQuietly(tempFolder.toFile());

        // Speech2Text request (async - long running)
        try (SpeechClient speechClient = SpeechClient.create()) {
            RecognitionConfig config =
                    RecognitionConfig.newBuilder()
                            .setEncoding(RecognitionConfig.AudioEncoding.FLAC)
                            .setSampleRateHertz(audioInfo.getSampleRate())
                            .setAudioChannelCount(audioInfo.getChannels())
                            .setLanguageCode(language)
                            .build();
            RecognitionAudio audio =
                    RecognitionAudio.newBuilder().setUri("gs://" + bucketName + "/" + gcsId).build();

            OperationFuture<LongRunningRecognizeResponse, LongRunningRecognizeMetadata> response =
                    speechClient.longRunningRecognizeAsync(config, audio);

            Metadata metadata = Metadata.create();
            metadata.add("language", Values.stringValue(language), Visibility.EMPTY);
            GoogleSchemaContribution.OPERATION_NAME
                    .setProperty(vertex, response.getName(), metadata,
                            Visibility.EMPTY, getAuthorizations());

            getGraph().flush();

            LOGGER.info("Submitted Google response operation with id %s", response.getName());
        }
    }

    private Path createFlac(Vertex vertex, Path folder) throws IOException {
        Path videoFile = Files.createFile(folder.resolve(S2TConstants.TEMP_VIDEO_NAME));
        Path finalFile = folder.resolve(S2TConstants.TEMP_FLAC_NAME);

        StreamingPropertyValue spv = BcSchema.RAW.getPropertyValue(vertex);
        IOUtils.copyLarge(spv.getInputStream(), new FileOutputStream(videoFile.toFile()));

        FFmpegBuilder builder = new FFmpegBuilder();
        builder.addInput(videoFile.toAbsolutePath().toString());
        builder.addOutput(finalFile.toAbsolutePath().toString());

        FFmpegExecutor executor = new FFmpegExecutor(AVUtils.ffmpeg());
        FFmpegJob job = executor.createJob(builder);
        job.run();

        return finalFile;
    }

    private void uploadObjectToGCS(String objectName, String filePath) throws IOException {
        Storage storage = StorageOptions.newBuilder().setProjectId(GoogleCredentialUtils.getProjectId()).build().getService();
        BlobId blobId = BlobId.of(bucketName, objectName);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
        storage.create(blobInfo, Files.readAllBytes(Paths.get(filePath)));
        LOGGER.info("File %s uploaded to bucket %s as %s", filePath, bucketName, objectName);
    }

    class AudioInfo {
        private static final int DEFAULT_SAMPLE_RATE = 44100;
        private static final int DEFAULT_NUM_CHANNELS = 2;

        private int sampleRate;
        private int channels;
        private double duration;

        AudioInfo(String filePath) {
            initialize(filePath);
        }

        private void initialize(String filePath) {
            FFmpegProbeResult probe = AVMediaInfo.probe(filePath);
            this.sampleRate = DEFAULT_SAMPLE_RATE;
            this.channels = DEFAULT_NUM_CHANNELS;
            if (probe != null && !probe.getStreams().isEmpty()) {
                this.sampleRate = probe.getStreams().get(0).sample_rate;
                this.channels = probe.getStreams().get(0).channels;
                this.duration = probe.getStreams().get(0).duration;
            }
        }

        int getSampleRate() {
            return sampleRate;
        }

        int getChannels() {
            return channels;
        }
    }
}
