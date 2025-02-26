package io.bigconnect.dw.google.speech;

import com.fasterxml.jackson.annotation.JsonProperty;
import retrofit2.Call;
import okhttp3.MultipartBody;
import okhttp3.RequestBody;
import retrofit2.http.Multipart;
import retrofit2.http.POST;
import retrofit2.http.Part;
import com.google.inject.Inject;
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
import lombok.Getter;
import lombok.Setter;
import net.bramp.ffmpeg.FFmpegExecutor;
import net.bramp.ffmpeg.builder.FFmpegBuilder;
import net.bramp.ffmpeg.builder.FFmpegOutputBuilder;
import net.bramp.ffmpeg.job.FFmpegJob;
import okhttp3.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static io.bigconnect.dw.google.speech.Speech2TextSchemaContribution.GOOGLE_S2T_DONE_PROPERTY;
import static io.bigconnect.dw.google.speech.Speech2TextSchemaContribution.GOOGLE_S2T_PROGRESS_PROPERTY;

@Name("Speech to Text Worker")
@Description("Performs Speech to Text transcription using VLLM services")
public class Speech2TextLongRunningProcessWorker extends LongRunningProcessWorker {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(Speech2TextLongRunningProcessWorker.class);
    public static final String CONFIG_URL = "vllm.url";
    public static final String CONFIG_API_KEY = "vllm.api.key";
    public static final String CONFIG_TIMEOUT = "s2t.timeout.seconds";
    private static final int DEFAULT_TIMEOUT_SECONDS = 36000;

    private final LongRunningProcessRepository longRunningProcessRepository;
    private final Graph graph;
    private final Configuration configuration;
    private final SchemaRepository schemaRepository;
    private final WorkQueueRepository workQueueRepository;
    private final WebQueueRepository webQueueRepository;
    private SpeechToTextService service;

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

        String baseUrl = configuration.get(CONFIG_URL, null);
        String apiKey = configuration.get(CONFIG_API_KEY, null);
        int timeoutSeconds;
        try {
            timeoutSeconds = Integer.parseInt(configuration.get(CONFIG_TIMEOUT, String.valueOf(DEFAULT_TIMEOUT_SECONDS)));
        } catch (NumberFormatException e) {
            LOGGER.warn("Invalid timeout value in configuration, using default: " + DEFAULT_TIMEOUT_SECONDS);
            timeoutSeconds = DEFAULT_TIMEOUT_SECONDS;
        }

        Preconditions.checkState(!StringUtils.isEmpty(baseUrl),
                "Please provide the '" + CONFIG_URL + "' config parameter");

        OkHttpClient.Builder clientBuilder = new OkHttpClient.Builder()
                .connectTimeout(timeoutSeconds, TimeUnit.SECONDS)
                .readTimeout(timeoutSeconds, TimeUnit.SECONDS)
                .writeTimeout(timeoutSeconds, TimeUnit.SECONDS);

        if (!StringUtils.isEmpty(apiKey)) {
            clientBuilder.addInterceptor(chain -> {
                Request original = chain.request();
                Request request = original.newBuilder()
                        .header("Authorization", "Bearer " + apiKey)
                        .method(original.method(), original.body())
                        .build();
                return chain.proceed(request);
            });
        }

        Retrofit retrofit = new Retrofit.Builder()
                .baseUrl(baseUrl)
                .client(clientBuilder.build())
                .addConverterFactory(JacksonConverterFactory.create())
                .build();

        service = retrofit.create(SpeechToTextService.class);
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
            Path audioPath = prepareAudioFile(vertex, tempFolder);
            float conversionTime = Math.round((System.currentTimeMillis() - start) / 1000f * 100f) / 100f;
            longRunningProcessRepository.reportProgress(itemJson, 0.2, "Conversion took " + conversionTime + "s. Processing audio...");

            // Create multipart request
            File audioFile = audioPath.toFile();
            RequestBody fileBody = RequestBody.create(MediaType.parse("audio/*"), audioFile);
            MultipartBody.Part filePart = MultipartBody.Part.createFormData("file", audioFile.getName(), fileBody);

            RequestBody languageBody = RequestBody.create(MediaType.parse("text/plain"), language);
            RequestBody saveOutputBody = RequestBody.create(MediaType.parse("text/plain"), "false");

            Response<SpeechToTextResponse> response = service.processAudio(filePart, languageBody, saveOutputBody).execute();

            if (response.isSuccessful() && response.body() != null) {
                SpeechToTextResponse result = response.body();

                if ("success".equals(result.getStatus())) {
                    PropertyMetadata propertyMetadata = new PropertyMetadata(
                            new SystemUser(), new VisibilityJson(), Visibility.EMPTY
                    );
                    propertyMetadata.add(BcSchema.TEXT_LANGUAGE_METADATA.getMetadataKey(),
                            Values.stringValue(language), Visibility.EMPTY);

                    BcSchema.TEXT.addPropertyValue(
                            vertex,
                            language,
                            DefaultStreamingPropertyValue.create(result.getTranscription()),
                            propertyMetadata.createMetadata(),
                            Visibility.EMPTY,
                            vertex.getAuthorizations()
                    );

                    webQueueRepository.pushTextUpdated(vertex.getId(), Priority.HIGH);

                    GOOGLE_S2T_PROGRESS_PROPERTY.setProperty(vertex, Boolean.FALSE, Visibility.EMPTY, vertex.getAuthorizations());
                    GOOGLE_S2T_DONE_PROPERTY.setProperty(vertex, Boolean.TRUE, Visibility.EMPTY, vertex.getAuthorizations());

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

                    longRunningProcessRepository.reportProgress(itemJson, 1.0, "Completed");
                } else {
                    longRunningProcessRepository.reportProgress(
                            itemJson.put("error", "Transcription failed"),
                            1.0,
                            "Error"
                    );
                }
            } else {
                longRunningProcessRepository.reportProgress(
                        itemJson.put("error", "Service call failed with status: " + response.code()),
                        1.0,
                        "Error"
                );
            }

            // Cleanup
            FileUtils.deleteQuietly(tempFolder.toFile());

        } catch (Exception ex) {
            LOGGER.error("Could not process audio file", ex);
            longRunningProcessRepository.reportProgress(
                    itemJson.put("error", ex.getMessage()),
                    1.0,
                    "Error"
            );
        }
    }

    private Path prepareAudioFile(Vertex vertex, Path folder) throws IOException {
        Path sourceFile = Files.createFile(folder.resolve(S2TConstants.TEMP_VIDEO_NAME));
        Path outputFile = folder.resolve(S2TConstants.TEMP_FLAC_NAME);

        StreamingPropertyValue spv = BcSchema.RAW.getPropertyValue(vertex);
        IOUtils.copyLarge(spv.getInputStream(), Files.newOutputStream(sourceFile.toFile().toPath()));

        // Check source file size
        long sourceSize = Files.size(sourceFile);
        if (sourceSize > 100 * 1024 * 1024) { // If larger than 100MB
            LOGGER.warn("Source file is large: {} MB", sourceSize / (1024 * 1024));
        }

        String mimeType = BcSchema.MIME_TYPE.getFirstPropertyValue(vertex);
        FFmpegBuilder builder = new FFmpegBuilder();
        builder.addExtraArgs("-vn"); // No video
        builder.addExtraArgs("-sn"); // No subtitles

        builder.addInput(sourceFile.toAbsolutePath().toString());

        if (sourceSize > 100 * 1024 * 1024) {
            // For large files, use more compression
            builder.addOutput(new FFmpegOutputBuilder()
                    .setFilename(outputFile.toAbsolutePath().toString())
                    .setAudioCodec("flac")
                    .setAudioChannels(1)
                    .setAudioSampleRate(16000) // Lower sample rate for speech
                    .addExtraArgs("-compression_level", "8") // Higher compression
            );
        } else {
            // For smaller files, use standard settings
            builder.addOutput(new FFmpegOutputBuilder()
                    .setFilename(outputFile.toAbsolutePath().toString())
                    .setAudioCodec("flac")
                    .setAudioChannels(1)
                    .addExtraArgs("-compression_level", "0")
            );
        }

        FFmpegExecutor executor = new FFmpegExecutor(AVUtils.ffmpeg());
        FFmpegJob job = executor.createJob(builder);
        job.run();

        // Verify output size
        long outputSize = Files.size(outputFile);
        LOGGER.info("Converted file size: {} MB", outputSize / (1024 * 1024));

        return outputFile;
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
}

interface SpeechToTextService {
    @Multipart
    @POST("v1/speech-to-text")
    Call<SpeechToTextResponse> processAudio(
            @Part MultipartBody.Part file,
            @Part("language") RequestBody language,
            @Part("save_output") RequestBody saveOutput
    );
}

@Setter
@Getter
class SpeechToTextResponse {
    private String status;

    private String transcription;

    private String language;

    @JsonProperty("output_file")
    private String outputFile;

    private long timestamp;

}