package io.bigconnect.dw.google.vision;

import com.github.pemistahl.lingua.api.IsoCode639_1;
import com.google.cloud.vision.v1.*;
import com.google.protobuf.ByteString;
import com.mware.core.ingest.dataworker.DataWorkerData;
import com.mware.core.ingest.dataworker.DataWorkerPrepareData;
import com.mware.core.ingest.dataworker.ElementOrPropertyStatus;
import com.mware.core.ingest.dataworker.PostMimeTypeWorker;
import com.mware.core.model.Description;
import com.mware.core.model.Name;
import com.mware.core.model.clientapi.dto.VisibilityJson;
import com.mware.core.model.properties.BcSchema;
import com.mware.core.model.properties.RawObjectSchema;
import com.mware.core.model.properties.types.PropertyMetadata;
import com.mware.core.model.workQueue.Priority;
import com.mware.core.user.SystemUser;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.ge.Authorizations;
import com.mware.ge.Element;
import com.mware.ge.Metadata;
import com.mware.ge.Visibility;
import com.mware.ge.mutation.ExistingElementMutation;
import com.mware.ge.values.storable.DefaultStreamingPropertyValue;
import io.bigconnect.dw.text.common.LanguageDetectorUtil;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

@Name("Google Text Detection Data Worker")
@Description("Extracts text from images")
public class TextDetectorPostMimeTypeWorker extends PostMimeTypeWorker {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(TextDetectorPostMimeTypeWorker.class);

    private ImageAnnotatorClient client;
    private LanguageDetectorUtil languageDetector;

    @Override
    public void prepare(DataWorkerPrepareData workerPrepareData) throws Exception {
        super.prepare(workerPrepareData);

        client = ImageAnnotatorClient.create();
        languageDetector = new LanguageDetectorUtil();
    }

    @Override
    public void execute(String mimeType, DataWorkerData data, Authorizations authorizations) throws Exception {
        if (!mimeType.startsWith("image")) {
            return;
        }

        File imageFile = getLocalFileForRaw(data.getElement());
        if (imageFile == null) {
            return;
        }

        List<AnnotateImageRequest> requests = new ArrayList<>();
        ByteString imgBytes = ByteString.readFrom(new FileInputStream(imageFile));
        Image img = Image.newBuilder().setContent(imgBytes).build();
        Feature feat = Feature.newBuilder().setType(Feature.Type.TEXT_DETECTION).build();
        AnnotateImageRequest request =
                AnnotateImageRequest.newBuilder().addFeatures(feat).setImage(img).build();
        requests.add(request);

        BatchAnnotateImagesResponse response = client.batchAnnotateImages(requests);
        List<AnnotateImageResponse> responses = response.getResponsesList();

        StringBuilder builder = new StringBuilder();
        for (AnnotateImageResponse res : responses) {
            if (res.hasError()) {
                LOGGER.error("Error extracting text from image: %s", res.getError().getMessage());
                return;
            }

            for (EntityAnnotation annotation : res.getTextAnnotationsList()) {
                builder.append(annotation.getDescription())
                        .append(" ");
            }
        }

        String text = builder.toString().trim();
        if (text.length() == 0)
            return;

        Optional<IsoCode639_1> lang = languageDetector.detectLanguage(text);
        Element element = refresh(data.getElement(), authorizations);
        ExistingElementMutation<Element> m = element.prepareMutation();

        PropertyMetadata propertyMetadata = new PropertyMetadata(new SystemUser(), new VisibilityJson(), Visibility.EMPTY);
        BcSchema.MIME_TYPE_METADATA.setMetadata(propertyMetadata, "text/plain", Visibility.EMPTY);
        BcSchema.TEXT_DESCRIPTION_METADATA.setMetadata(propertyMetadata, "Text", Visibility.EMPTY);

        if (lang.isPresent()) {
            String l = lang.get().name().toLowerCase();
            RawObjectSchema.RAW_LANGUAGE.addPropertyValue(m, l, l,
                    propertyMetadata.createMetadata(), Visibility.EMPTY);
            BcSchema.TEXT_LANGUAGE_METADATA.setMetadata(propertyMetadata, l, Visibility.EMPTY);
        } else {
            BcSchema.TEXT_LANGUAGE_METADATA.setMetadata(propertyMetadata, Locale.ENGLISH.getLanguage(), Visibility.EMPTY);
        }

        Metadata textMetadata = propertyMetadata.createMetadata();
        BcSchema.TEXT.addPropertyValue(
                m,
                "",
                DefaultStreamingPropertyValue.create(text),
                textMetadata,
                Visibility.EMPTY
        );

        m.save(authorizations);
        getGraph().flush();

        if (lang.isPresent()) {
            getWorkQueueRepository().pushOnDwQueue(
                    element,
                    "",
                    RawObjectSchema.RAW_LANGUAGE.getPropertyName(),
                    data.getWorkspaceId(),
                    data.getVisibilitySource(),
                    Priority.HIGH,
                    ElementOrPropertyStatus.UPDATE,
                    null
            );
        }

        getWebQueueRepository().pushTextUpdated(data.getElement().getId(), Priority.HIGH);
    }
}
