package io.bigconnect.dw.google.speech;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.api.gax.rpc.ApiException;
import com.google.cloud.speech.v1.*;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.inject.Inject;
import com.google.longrunning.Operation;
import com.google.longrunning.OperationsClient;
import com.google.protobuf.InvalidProtocolBufferException;
import com.mware.bigconnect.ffmpeg.AVMediaInfo;
import com.mware.bigconnect.ffmpeg.AVUtils;
import com.mware.core.config.Configuration;
import com.mware.core.ingest.dataworker.ElementOrPropertyStatus;
import com.mware.core.model.Description;
import com.mware.core.model.Name;
import com.mware.core.model.clientapi.dto.VisibilityJson;
import com.mware.core.model.longRunningProcess.LongRunningProcessRepository;
import com.mware.core.model.longRunningProcess.LongRunningProcessWorker;
import com.mware.core.model.longRunningProcess.LongRunningWorkerPrepareData;
import com.mware.core.model.properties.BcSchema;
import com.mware.core.model.properties.MediaBcSchema;
import com.mware.core.model.properties.RawObjectSchema;
import com.mware.core.model.properties.types.PropertyMetadata;
import com.mware.core.model.role.GeAuthorizationRepository;
import com.mware.core.model.schema.SchemaProperty;
import com.mware.core.model.schema.SchemaRepository;
import com.mware.core.model.workQueue.Priority;
import com.mware.core.model.workQueue.WebQueueRepository;
import com.mware.core.model.workQueue.WorkQueueRepository;
import com.mware.core.user.SystemUser;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.core.util.ClientApiConverter;
import com.mware.ge.*;
import com.mware.ge.util.Preconditions;
import com.mware.ge.values.storable.DefaultStreamingPropertyValue;
import com.mware.ge.values.storable.StreamingPropertyValue;
import com.mware.ge.values.storable.Value;
import com.mware.ge.values.storable.Values;
import io.bigconnect.dw.google.common.schema.GoogleCredentialUtils;
import net.bramp.ffmpeg.FFmpegExecutor;
import net.bramp.ffmpeg.builder.FFmpegBuilder;
import net.bramp.ffmpeg.builder.FFmpegOutputBuilder;
import net.bramp.ffmpeg.job.FFmpegJob;
import net.bramp.ffmpeg.probe.FFmpegProbeResult;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.UUID;

import static io.bigconnect.dw.google.speech.Speech2TextSchemaContribution.GOOGLE_S2T_DONE_PROPERTY;
import static io.bigconnect.dw.google.speech.Speech2TextSchemaContribution.GOOGLE_S2T_PROGRESS_PROPERTY;

@Name("Google Speech2Text Data Worker")
@Description("Performs Speech2Text on Audio/Video files using Google Cloud Services")
public class Speech2TextLongRunningProcessWorker extends LongRunningProcessWorker {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(Speech2TextLongRunningProcessWorker.class);
    private static final int CHECK_INTERVAL = 10; //seconds
    static final String CONFIG_GOOGLE_S2T_BUCKET_NAME = "google.s2t.bucket.name";

    private final LongRunningProcessRepository longRunningProcessRepository;
    private final Graph graph;
    private final Configuration configuration;
    private final SchemaRepository schemaRepository;
    private final WorkQueueRepository workQueueRepository;
    private final WebQueueRepository webQueueRepository;

    private String bucketName;

    @Inject
    public Speech2TextLongRunningProcessWorker(
            LongRunningProcessRepository longRunningProcessRepository,
            Graph graph,
            Configuration configuration,
            SchemaRepository schemaRepository,
            WorkQueueRepository workQueueRepository,
            WebQueueRepository webQueueRepository
    ) {
        this.longRunningProcessRepository = longRunningProcessRepository;
        this.graph = graph;
        this.configuration = configuration;
        this.schemaRepository = schemaRepository;
        this.workQueueRepository = workQueueRepository;
        this.webQueueRepository = webQueueRepository;
    }

    @Override
    public void prepare(LongRunningWorkerPrepareData workerPrepareData) {
        super.prepare(workerPrepareData);

        this.bucketName = configuration.get(CONFIG_GOOGLE_S2T_BUCKET_NAME, "");
        Preconditions.checkState(!StringUtils.isEmpty(bucketName),
                "Please provide the " + CONFIG_GOOGLE_S2T_BUCKET_NAME + " configuration property");
    }

    @Override
    public boolean isHandled(JSONObject queueItem) {
        return queueItem.getString("type").equals(Speech2TextQueueItem.TYPE);
    }

    @Override
    protected void processInternal(JSONObject itemJson) {
        Speech2TextQueueItem queueItem = ClientApiConverter
                .toClientApi(itemJson.toString(), Speech2TextQueueItem.class);

        final Authorizations authorizations = graph.createAuthorizations(queueItem.getAuthorizations());
        final Vertex vertex = graph.getVertex(queueItem.getVertexId(), authorizations);
        final Property languageProp = RawObjectSchema.RAW_LANGUAGE.getFirstProperty(vertex);
        final SchemaProperty prop = schemaRepository.getPropertyByName(RawObjectSchema.RAW_LANGUAGE.getPropertyName());

        try {
            Path tempFolder = Files.createTempDirectory(S2TConstants.SPEECH_TEMP_DIR_PREFIX);

            if (languageProp == null) {
                longRunningProcessRepository.reportProgress(
                        itemJson.put("error", prop.getDisplayName() + " is not set"),
                        1.0,
                        "Error"
                );
                return;
            }

            final String language = languageProp.getValue().asObjectCopy().toString();
            if (StringUtils.isEmpty(language)) {
                longRunningProcessRepository.reportProgress(
                        itemJson.put("error", prop.getDisplayName() + " is empty"),
                        1.0,
                        "Error"
                );
                return;
            }

            GOOGLE_S2T_PROGRESS_PROPERTY.setProperty(vertex, Boolean.TRUE, Visibility.EMPTY, vertex.getAuthorizations());
            graph.flush();

            // Convert to FLAC audio
            longRunningProcessRepository.reportProgress(itemJson, 0.1, "Converting file...");
            long start = System.currentTimeMillis();
            Path audioPath = createFlac(vertex, tempFolder);
            float conversionTime = Math.round((System.currentTimeMillis() - start) / 1000f * 100f) / 100f;
            longRunningProcessRepository.reportProgress(itemJson, 0.2, "Conversion took " + conversionTime + "s. Uploading to Google...");

            // Probe Audio info
            final AudioInfo audioInfo = new AudioInfo(audioPath.toAbsolutePath().toString());
            // Upload to GCS
            final String gcsId = UUID.randomUUID().toString();
            start = System.currentTimeMillis();
            uploadObjectToGCS(gcsId, audioPath.toAbsolutePath().toString());
            float uploadTime = Math.round((System.currentTimeMillis() - start) / 1000f * 100f) / 100f;
            longRunningProcessRepository.reportProgress(itemJson, 0.3, "Upload took " + uploadTime + "s. Asking Google to perform speech recognition...");

            // Cleanup
            FileUtils.deleteQuietly(tempFolder.toFile());

            // Speech2Text request (async - long running)
            String opName = null;
            try (final SpeechClient speechClient = SpeechClient.create()) {
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
                opName = response.getName();
                LOGGER.info("Submitted Google response operation with id %s", response.getName());
            }

            int secondCounter = 0;
            while (!checkResult(vertex, language, itemJson, opName)) {
                Thread.sleep(CHECK_INTERVAL * 1000);
                secondCounter += CHECK_INTERVAL;
                if (secondCounter > 60_000) {
                    // just to be safe, cancel the thread after 1h
                    longRunningProcessRepository.reportProgress(itemJson, 1.0, "Dangling task cancelled");
                    break;
                }
            }
        } catch (Exception ex) {
            LOGGER.error("Could not cut video clip!", ex);
            longRunningProcessRepository.reportProgress(itemJson.put("error", ex.getMessage()), 1.0, "Error");
        }
    }

    private boolean checkResult(Vertex vertex, String language, JSONObject itemJson, String operationName) {
        String lrpId = itemJson.getString("id");

        try (final SpeechClient speechClient = SpeechClient.create()) {
            try (OperationsClient client = speechClient.getOperationsClient()) {
                final Storage storage = StorageOptions.newBuilder().setProjectId(GoogleCredentialUtils.getProjectId()).build().getService();
                try {
                    Operation operation = client.getOperation(operationName);
                    LongRunningRecognizeMetadata longRunningRecognizeMetadata =
                            LongRunningRecognizeMetadata.parseFrom(operation.getMetadata().getValue());

                    if (operation.getDone()) {
                        try {
                            StringBuilder resultedText = new StringBuilder();
                            com.google.cloud.speech.v1p1beta1.LongRunningRecognizeResponse opResponse = com.google.cloud.speech.v1p1beta1.LongRunningRecognizeResponse.parseFrom(
                                    operation.getResponse().getValue()
                            );
                            if (opResponse.getResultsCount() > 0) {
                                for (int i = 0; i < opResponse.getResultsCount(); i++) {
                                    resultedText.append(" ")
                                            .append(opResponse.getResults(i).getAlternatives(0).getTranscript());
                                }
                            }

                            PropertyMetadata propertyMetadata = new PropertyMetadata(
                                    new SystemUser(), new VisibilityJson(), Visibility.EMPTY
                            );
                            propertyMetadata.add(BcSchema.TEXT_LANGUAGE_METADATA.getMetadataKey(), Values.stringValue(language), Visibility.EMPTY);
                            BcSchema.TEXT.addPropertyValue(
                                    vertex,
                                    language,
                                    DefaultStreamingPropertyValue.create(resultedText.toString()),
                                    propertyMetadata.createMetadata(), Visibility.EMPTY, vertex.getAuthorizations()
                            );

                            // add also the new language
                            RawObjectSchema.RAW_LANGUAGE.addPropertyValue(vertex, language, language,
                                    null, Visibility.EMPTY, vertex.getAuthorizations());

                            webQueueRepository.pushTextUpdated(vertex.getId(), Priority.HIGH);

                            GOOGLE_S2T_PROGRESS_PROPERTY.setProperty(vertex, Boolean.FALSE, Visibility.EMPTY, vertex.getAuthorizations());
                            GOOGLE_S2T_DONE_PROPERTY.setProperty(vertex, Boolean.TRUE, Visibility.EMPTY, vertex.getAuthorizations());

                            // Cleanup
                            graph.flush();

                            logElement(vertex);

                            workQueueRepository.pushOnDwQueue(
                                    vertex,
                                    language,
                                    RawObjectSchema.RAW_LANGUAGE.getPropertyName(),
                                    null,
                                    null,
                                    Priority.HIGH,
                                    ElementOrPropertyStatus.UPDATE,
                                    null
                            );

                            long spent = Instant.now().getEpochSecond() - longRunningRecognizeMetadata.getStartTime().getSeconds();
                            longRunningProcessRepository.reportProgress(lrpId, 0.96, "Finished in " + spent + "s. Cleaning up...");

                            final String meta = client.getOperation(operationName).getMetadata().getValue().toStringUtf8();
                            if (!StringUtils.isEmpty(meta)) {
                                String[] gcsUrl = meta.split("/");
                                if (gcsUrl.length > 0) {
                                    storage.delete(bucketName, gcsUrl[gcsUrl.length - 1]);
                                }
                            }
                            longRunningProcessRepository.reportProgress(lrpId, 1, "Completed");
                            return true;
                        } catch (InvalidProtocolBufferException e) {
                            itemJson.put("error", e.getMessage());
                            longRunningProcessRepository.reportProgress(lrpId, 1.0, "Google protocol error");
                            return false;
                        }
                    } else {
                        double progressPercent = 0.3 // previously reported progress
                                + (longRunningRecognizeMetadata.getProgressPercent() * 0.65 / 100f);
                        long spent = Instant.now().getEpochSecond() - longRunningRecognizeMetadata.getStartTime().getSeconds();
                        longRunningProcessRepository.reportProgress(lrpId, progressPercent, "Google progress: " + longRunningRecognizeMetadata.getProgressPercent() + "%, seconds: " + spent);
                        return false;
                    }
                } catch (ApiException | InvalidProtocolBufferException ex) {
                    itemJson.put("error", ex.getMessage());
                    longRunningProcessRepository.reportProgress(lrpId, 1.0, "Google protocol error");
                    return false;
                }
            }
        } catch (IOException e) {
            itemJson.put("error", e.getMessage());
            longRunningProcessRepository.reportProgress(lrpId, 1.0, "Google protocol error");
            return false;
        }
    }

    private void uploadObjectToGCS(String objectName, String filePath) throws IOException {
        Storage storage = StorageOptions.newBuilder().setProjectId(GoogleCredentialUtils.getProjectId()).build().getService();
        BlobId blobId = BlobId.of(bucketName, objectName);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
        storage.create(blobInfo, Files.readAllBytes(Paths.get(filePath)));
        LOGGER.info("File %s uploaded to bucket %s as %s", filePath, bucketName, objectName);
    }

    private Path createFlac(Vertex vertex, Path folder) throws IOException {
        Path videoFile = Files.createFile(folder.resolve(S2TConstants.TEMP_VIDEO_NAME));
        Path finalFile = folder.resolve(S2TConstants.TEMP_FLAC_NAME);

        StreamingPropertyValue spv = BcSchema.RAW.getPropertyValue(vertex);
        IOUtils.copyLarge(spv.getInputStream(), new FileOutputStream(videoFile.toFile()));

        FFmpegBuilder builder = new FFmpegBuilder();
        builder.addExtraArgs("-vn");
        builder.addExtraArgs("-sn");

        builder.addInput(videoFile.toAbsolutePath().toString());
        builder.addOutput(new FFmpegOutputBuilder()
                .setFilename(finalFile.toAbsolutePath().toString())
                .setAudioCodec("flac")
                .setAudioChannels(1)
                .addExtraArgs("-compression_level", "0")
        );

        FFmpegExecutor executor = new FFmpegExecutor(AVUtils.ffmpeg());
        FFmpegJob job = executor.createJob(builder);
        job.run();

        return finalFile;
    }

    private void logElement(Element element) {
        final String SEPARATOR = "|$";
        Vertex v = (Vertex) element;
        Value duration = v.getPropertyValue(MediaBcSchema.MEDIA_DURATION.getPropertyName());

        StringBuilder sb = new StringBuilder();
        sb
                .append('\n')
                .append("gS2TLog_4874450843").append(SEPARATOR)
                .append(v.getId()).append(SEPARATOR)
                .append(duration != null ? duration.prettyPrint() : 0).append(SEPARATOR)
                .append(v.getPropertyValue("createdDate")).append(SEPARATOR)
                .append(v.getTimestamp());

        LOGGER.warn(sb.toString());
    }

    static class AudioInfo {
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
