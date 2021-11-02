package io.bigconnect.dw.google.vision;

import com.google.cloud.vision.v1.*;
import com.google.protobuf.ByteString;
import com.mware.bigconnect.image.ImageTagger;
import com.mware.core.ingest.dataworker.DataWorkerData;
import com.mware.core.ingest.dataworker.DataWorkerPrepareData;
import com.mware.core.ingest.dataworker.PostMimeTypeWorker;
import com.mware.core.model.Description;
import com.mware.core.model.Name;
import com.mware.core.model.termMention.TermMentionRepository;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.ge.Authorizations;
import com.mware.ge.Element;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;

@Name("Google Text Object Detector")
@Description("Tags objects in images")
public class ObjectDetectorPostMimeTypeWorker extends PostMimeTypeWorker {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(ObjectDetectorPostMimeTypeWorker.class);

    private ImageAnnotatorClient client;

    @Override
    public void prepare(DataWorkerPrepareData workerPrepareData) throws Exception {
        super.prepare(workerPrepareData);
        client = ImageAnnotatorClient.create();
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
        Feature feat = Feature.newBuilder().setType(Feature.Type.OBJECT_LOCALIZATION).build();
        AnnotateImageRequest request =
                AnnotateImageRequest.newBuilder().addFeatures(feat).setImage(img).build();
        requests.add(request);

        BatchAnnotateImagesResponse response = client.batchAnnotateImages(requests);
        List<AnnotateImageResponse> responses = response.getResponsesList();

        Element e = refresh(data.getElement(), authorizations);

        ImageTagger imageTagger = new ImageTagger(getConfiguration(), getGraph(), getSchemaRepository(), authorizations,
                getWorkQueueRepository(), getWebQueueRepository(),
                new TermMentionRepository(getGraph(), getGraphAuthorizationRepository()));

        for (AnnotateImageResponse res : responses) {
            if (res.hasError()) {
                LOGGER.error("Error tagging objects in image: %s", res.getError().getMessage());
                return;
            }

            for (LocalizedObjectAnnotation entity : res.getLocalizedObjectAnnotationsList()) {
                List<NormalizedVertex> box = entity.getBoundingPoly().getNormalizedVerticesList();
                imageTagger.createObject(
                        e,
                        box.get(0).getX(),
                        box.get(0).getY(),
                        box.get(2).getX(),
                        box.get(2).getY(),
                        entity.getScore(),
                        entity.getName()
                );
            }
        }
    }
}
