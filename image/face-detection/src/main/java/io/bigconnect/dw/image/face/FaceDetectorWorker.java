package io.bigconnect.dw.image.face;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.mware.core.ingest.dataworker.DataWorker;
import com.mware.core.ingest.dataworker.DataWorkerData;
import com.mware.core.ingest.dataworker.DataWorkerPrepareData;
import com.mware.core.model.Description;
import com.mware.core.model.Name;
import com.mware.core.model.properties.ArtifactDetectedObject;
import com.mware.core.model.properties.BcSchema;
import com.mware.core.model.properties.MediaBcSchema;
import com.mware.core.model.schema.SchemaConstants;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.ge.Element;
import com.mware.ge.Property;
import com.mware.ge.Vertex;
import com.mware.ge.Visibility;
import com.mware.ge.mutation.ElementMutation;
import com.mware.ge.query.QueryResultsIterable;
import com.mware.ge.store.StorableVertex;
import com.mware.ge.util.IOUtils;
import com.mware.ge.util.Preconditions;
import com.mware.ge.values.storable.StreamingPropertyValue;
import com.mware.ge.values.storable.Values;
import okhttp3.MediaType;
import okhttp3.MultipartBody;
import okhttp3.RequestBody;
import org.apache.commons.lang3.StringUtils;
import retrofit2.Call;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;
import retrofit2.http.*;

import java.io.InputStream;
import java.util.*;

@Name("Face Detector")
@Description("Detects faces in images")
public class FaceDetectorWorker extends DataWorker {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(FaceDetectorWorker.class);
    public static final String CONFIG_URL = "face-detector.url";
    public static final String CONFIG_BASE_URL = "base-compre-face.url";
    public static final String CONFIG_API_KEY = "face-detector.api.key";
    public static final String CONFIDENCE_THRESHOLD = "compre-face.threshold";
    private ProcessedImageInfo lastProcessedImage;
    private CompreFaceService compreFaceService;
    private FaceDetectorService faceService;

    @Override
    public void prepare(DataWorkerPrepareData workerPrepareData) throws Exception {
        super.prepare(workerPrepareData);

        // Use the exact config keys from your configuration
        String baseUrl = getConfiguration().get(CONFIG_BASE_URL, null);
        String faceUrl = getConfiguration().get(CONFIG_URL, null);

        Preconditions.checkState(!StringUtils.isEmpty(faceUrl),
                "Please provide the '" + CONFIG_URL + "' config parameter");
        Preconditions.checkState(!StringUtils.isEmpty(baseUrl),
                "Please provide the 'base-compre-face.url' config parameter");

        // Setup Face Detection service
        Retrofit faceRetrofit = new Retrofit.Builder()
                .baseUrl(faceUrl)
                .addConverterFactory(JacksonConverterFactory.create())
                .build();
        faceService = faceRetrofit.create(FaceDetectorService.class);

        // Setup CompreFace service
        Retrofit compreFaceRetrofit = new Retrofit.Builder()
                .baseUrl(baseUrl) // Using base-compre-face.url
                .addConverterFactory(JacksonConverterFactory.create())
                .build();
        compreFaceService = compreFaceRetrofit.create(CompreFaceService.class);

        // Add validation
        Preconditions.checkNotNull(compreFaceService, "CompreFace service was not properly initialized");
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
        Element element = refresh(data.getElement());
        String elementId = element.getId();

        StreamingPropertyValue spv = BcSchema.RAW.getPropertyValue(element);
        if (spv == null) {
            LOGGER.warn("Could not find image data property for element {}", elementId);
            return;
        }

        int width = MediaBcSchema.MEDIA_WIDTH.getPropertyValue(element);
        int height = MediaBcSchema.MEDIA_HEIGHT.getPropertyValue(element);
        String apiKey = getConfiguration().get(CONFIG_API_KEY, "");
        String title = BcSchema.TITLE.getFirstPropertyValue(element);
        String conceptType = ((StorableVertex) element).getConceptType();
        byte[] imageData = IOUtils.toBytes(spv.getInputStream());

        // Create current image info and check for duplicates
        ProcessedImageInfo currentImage = new ProcessedImageInfo(title, imageData, elementId);

        // Thread-safe check for duplicate processing
        synchronized (this) {
            if (lastProcessedImage != null && lastProcessedImage.matches(currentImage)) {
                LOGGER.info("Skipping duplicate image processing for title: '{}', elementId: {}",
                        title, elementId);
                return;
            }

            // Update last processed image
            lastProcessedImage = currentImage;
        }

        try {
            // Step 1: Face Detection
            final ElementMutation<Vertex> m = element.prepareMutation();

            // Clear existing properties
            MediaBcSchema.DETECTED_OBJECT.getProperties(element).forEach(p -> {
                MediaBcSchema.DETECTED_OBJECT.removeProperty(m, p.getKey(), p.getVisibility());
            });
            FaceDetectorSchemaContribution.PERSON_AGE.removeProperty(m, Visibility.EMPTY);
            FaceDetectorSchemaContribution.PERSON_SEX.removeProperty(m, Visibility.EMPTY);

            RequestBody faceDetectBody = RequestBody.create(MediaType.parse("image/jpeg"), imageData);
            Response<FaceDetectorResponse> response = faceService.process(faceDetectBody).execute();
            FaceDetectorResponse result = response.body();
            Map<String, ArtifactDetectedObject> detectedFaces = new HashMap<>();

            if (result != null) {
                for (FaceDetectorResponse.FaceDetectorFace item : result.faces) {
                    int hash = Objects.hash(item.box.x1, item.box.y1, item.box.x2, item.box.y2, item.score);
                    String hashStr = String.valueOf(hash);
                    ArtifactDetectedObject artifact = new ArtifactDetectedObject(
                            item.box.x1 / width,
                            item.box.y1 / height,
                            item.box.x2 / width,
                            item.box.y2 / height,
                            SchemaConstants.CONCEPT_TYPE_PERSON,
                            "Face Detection"
                    );
                    detectedFaces.put(hashStr, artifact);
                    MediaBcSchema.DETECTED_OBJECT.addPropertyValue(m, hashStr, artifact, Visibility.EMPTY);
                    FaceDetectorSchemaContribution.PERSON_AGE.setProperty(m, item.age, Visibility.EMPTY);
                    FaceDetectorSchemaContribution.PERSON_SEX.setProperty(m, item.sex, Visibility.EMPTY);
                }
            }

            // Step 2: Process based on concept type
            if ("person".equalsIgnoreCase(conceptType) && !StringUtils.isEmpty(title)) {
                LOGGER.info("Processing person with title: {}", title);
                try {
                    // Create subject
                    SubjectRequest subjectRequest = new SubjectRequest(title);
                    Response<Void> subjectResponse = compreFaceService.createSubject(apiKey, subjectRequest).execute();
                    LOGGER.debug("Subject creation status for '{}': {}", title, subjectResponse.code());

                    // Add face to subject
                    RequestBody requestFile = RequestBody.create(MediaType.parse("image/jpeg"), imageData);
                    MultipartBody.Part filePart = MultipartBody.Part.createFormData("file", "image.jpg", requestFile);

                    Response<EmbeddingResponse> faceResponse = compreFaceService
                            .addFaceToSubject(apiKey, filePart, title)
                            .execute();

                    if (faceResponse.isSuccessful()) {
                        LOGGER.debug("Successfully added face to subject '{}'", title);
                    } else {
                        LOGGER.warn("Failed to add face to subject '{}', status: {}", title, faceResponse.code());
                    }
                } catch (Exception e) {
                    LOGGER.warn("Failed to create subject or add face --- for '{}': {}", title, e.getMessage());
                    e.printStackTrace();
                }
            } else if (("person".equalsIgnoreCase(conceptType) && StringUtils.isEmpty(title)) ||
                    "image".equalsIgnoreCase(conceptType)) {
                LOGGER.debug("Processing image for face detection and identification, elementId: {}", elementId);
                try {
                    detectAndIdentifyPerson(imageData, element);
                } catch (Exception e) {
                    LOGGER.warn("Failed to detect and identify faces in image {}: {}", elementId, e.getMessage());
                }
            } else {
                LOGGER.debug("Skipping processing for concept type: {}", conceptType);
            }

            // Save changes and flush graph
            try {
                m.save(getAuthorizations());
                getGraph().flush();
                LOGGER.debug("Successfully saved changes for element {}", elementId);
            } catch (Exception e) {
                LOGGER.error("Failed to save changes for element {}", elementId, e);
                throw e;
            }

        } catch (Exception e) {
            LOGGER.error("Error processing image for element {}", elementId, e);
            throw e;
        } finally {
            // Cleanup if needed
            try {
                if (spv.getInputStream() != null) {
                    spv.getInputStream().close();
                }
            } catch (Exception e) {
                LOGGER.warn("Error closing input stream for element {}", elementId, e);
            }
        }
    }


    private void detectAndIdentifyPerson(byte[] imageData, Element imageElement) {
        try {
            String apiKey = getConfiguration().get(CONFIG_API_KEY, "");
            String confidenceThresholdStr = getConfiguration().get(CONFIDENCE_THRESHOLD, String.valueOf(0.7));
            double confidenceThreshold = Double.parseDouble(confidenceThresholdStr);
            RequestBody requestFile = RequestBody.create(MediaType.parse("image/jpeg"), imageData);
            MultipartBody.Part filePart = MultipartBody.Part.createFormData("file", "image.jpg", requestFile);

            List<String> facePlugins = Arrays.asList("landmarks", "gender", "age", "pose");

            Response<RecognitionResponse> response = compreFaceService
                    .recognizeFace(apiKey, filePart, facePlugins)
                    .execute();

            if (response.isSuccessful() && response.body() != null) {
                RecognitionResponse result = response.body();

                if (result.result != null && !result.result.isEmpty()) {
                    //RecognitionResponse.Result faceResult = result.result.get(0);
                    String titles = "";
                    for(RecognitionResponse.Result faceResult: result.result)
                        if (faceResult.subjects != null && !faceResult.subjects.isEmpty()) {
                            RecognitionResponse.Result.Subject bestMatch = faceResult.subjects.get(0);

                            if (bestMatch.similarity >= confidenceThreshold) {
                                String matchedPersonName = bestMatch.subject;
                                Vertex personVertex = findPersonByName(matchedPersonName);

                                if (personVertex != null) {
                                    // Create entityHasImageRaw relationship (from old version)
                                    String entityHasImageEdgeLabel = "entityHasImageRaw";
                                    String entityHasImageEdgeId = String.format("%s_%s_entityHasImageRaw",
                                            personVertex.getId(), imageElement.getId());

                                    getGraph().addEdge(entityHasImageEdgeId,
                                            personVertex,
                                            (Vertex) imageElement,
                                            entityHasImageEdgeLabel,
                                            Visibility.EMPTY,
                                            getAuthorizations());

                                    LOGGER.info("Created entityHasImageRaw relationship between person {} and image",
                                            matchedPersonName);

                                    // Create rawContainsImageOfEntity relationships (from new version)
                                    Iterable<Property> detectedObjects = MediaBcSchema.DETECTED_OBJECT.getProperties(imageElement);

                                    for (Property detectedObject : detectedObjects) {
                                        String containsImageEdgeLabel = "rawContainsImageOfEntity";
                                        String containsImageEdgeId = String.format("%s_%s_%s",
                                                imageElement.getId(),
                                                detectedObject.getKey(),
                                                personVertex.getId());

                                        getGraph().addEdge(containsImageEdgeId,
                                                        (Vertex) imageElement,
                                                        personVertex,
                                                        containsImageEdgeLabel,
                                                        Visibility.EMPTY,
                                                        getAuthorizations())
                                                .setProperty("detectedObjectKey",
                                                        Values.stringValue(detectedObject.getName()),
                                                        Visibility.EMPTY,
                                                        getAuthorizations());

                                        LOGGER.info("Created rawContainsImageOfEntity relationship between detected face and person {} with key {}",
                                                matchedPersonName, detectedObject.getKey());
                                    }
                                    if (StringUtils.isNotBlank(matchedPersonName)){
                                        titles = titles.concat(matchedPersonName + " ");
                                    }
                                    // Update image title
                                    ElementMutation<Element> mutation = imageElement.prepareMutation();
                                    BcSchema.TITLE.addPropertyValue(mutation, "title", titles, Visibility.EMPTY);
                                    mutation.save(getAuthorizations());

                                    getGraph().flush();
                                } else {
                                    LOGGER.warn("Matched person {} not found in graph", matchedPersonName);
                                }
                            }
                        }
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error during face recognition and graph update", e);
            throw new RuntimeException("Failed to process face recognition and update graph", e);
        }
    }

    private Vertex findPersonByName(String personName) {
        try (QueryResultsIterable<Vertex> vertices = getGraph().query(getAuthorizations())
                .has(BcSchema.TITLE.getPropertyName(), Values.stringValue(personName))
                .hasConceptType(SchemaConstants.CONCEPT_TYPE_PERSON)  // If hasConceptType exists
                .limit(1)
                .vertices()) {

            return vertices.iterator().hasNext() ? vertices.iterator().next() : null;
        } catch (Exception e) {
            LOGGER.error("Error querying graph for person with title: {}", personName, e);
            return null;
        }
    }
}

