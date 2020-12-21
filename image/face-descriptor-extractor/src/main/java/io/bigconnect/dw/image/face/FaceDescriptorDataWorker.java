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
package io.bigconnect.dw.image.face;

import com.google.inject.Inject;
import com.mware.core.config.Configuration;
import com.mware.core.ingest.dataworker.DataWorker;
import com.mware.core.ingest.dataworker.DataWorkerData;
import com.mware.core.ingest.dataworker.DataWorkerPrepareData;
import com.mware.core.ingest.dataworker.ElementOrPropertyStatus;
import com.mware.core.model.Description;
import com.mware.core.model.Name;
import com.mware.core.model.properties.BcSchema;
import com.mware.core.model.properties.types.BooleanBcProperty;
import com.mware.core.model.properties.types.IntegerBcProperty;
import com.mware.core.model.properties.types.StringBcProperty;
import com.mware.core.model.workQueue.Priority;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.ge.*;
import com.mware.ge.values.storable.BooleanValue;
import com.mware.ge.values.storable.IntValue;
import com.mware.ge.values.storable.StreamingPropertyValue;
import com.mware.ge.values.storable.TextValue;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

@Name("Face Descriptor Data Worker")
@Description("Extracts face descriptors for face recognition process")
public class FaceDescriptorDataWorker extends DataWorker {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(FaceDescriptorDataWorker.class);
    private static final Boolean USE_CLASSIFIER = true;
    private static final BooleanBcProperty PROCESSED_PROPERTY = new BooleanBcProperty("processed");
    private static final String FACE_PERSON_RELATIONSHIP_NAME = "faceOf";
    private static final String DEFAULT_EXTRACTOR_URL = "http://127.0.0.1:5000/face";
    private static final String DEFAULT_DETECTOR_URL = "127.0.0.1:6060";
    private static final String DETECTOR_RELOAD_ENDPOINT = "/reload";
    private static final String EXTRACTOR_URL_CONF_KEY = "face.extractor.url";
    private static final String DETECTOR_URL_CONF_KEY = "face.detectors.url";

    public static final IntegerBcProperty FACE_NUMBER = new IntegerBcProperty("noFaces");
    public static final StringBcProperty FACE_DESCRIPTOR = new StringBcProperty("faceDescriptor");

    private String extractorUrl;
    private List<String> detectorsUrls;
    private final Configuration configuration;

    /**
     * Flow:
     * <p>
     * One-Shot training phase:
     * <p>
     * 1. Handle on 'faceOf' relationship -> Call face extractor URL for the 'Person' and image in it
     * If multiple descriptors for a person are present then AVG is computed
     * <p>
     * Detection phase:
     * <p>
     * 1. VGGFaces2 based python script runs in a separate process on a video stream
     * 2. All detected faces are compared with all the existing descriptors in BC (Retrieved as 'Face Descriptors' saved search)
     * 3. If a match is found then the '/face-event' endpoint is called -> 'Event' vertex linked with 'faceEvent' rel to 'Person'
     * <p>
     * NTH: Store face descriptors in faiss index
     */
    @Inject
    public FaceDescriptorDataWorker(Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    public void prepare(DataWorkerPrepareData workerPrepareData) throws Exception {
        super.prepare(workerPrepareData);
        this.extractorUrl = Optional.ofNullable(configuration.get(EXTRACTOR_URL_CONF_KEY, DEFAULT_EXTRACTOR_URL))
                .orElseThrow(() -> new GeException("'face.extractor.path' config property not found."));
        final String _urls = Optional.ofNullable(configuration.get(DETECTOR_URL_CONF_KEY, DEFAULT_DETECTOR_URL))
                .orElseThrow(() -> new GeException("'face.detectors.url' config property not found."));
        this.detectorsUrls = Arrays.asList(_urls.split(","));

        try {
            new URL(extractorUrl);
        } catch (MalformedURLException e) {
            throw new GeException("Value configured for 'face.extractor.url' is not a valid URL.");
        }
    }

    @Override
    public boolean isHandled(Element element, Property property) {
        if (element instanceof Edge) {
            Edge edge = (Edge) element;
            if (FACE_PERSON_RELATIONSHIP_NAME.equals(edge.getLabel())) {
                Property processed = edge.getProperty(PROCESSED_PROPERTY.getPropertyName());
                if (processed != null) {
                    return !((BooleanValue) processed.getValue()).booleanValue();
                } else {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public void execute(InputStream in, DataWorkerData data) throws Exception {
        LOGGER.debug("Executing Face Descriptor Data Worker");
        Edge edge = (Edge) data.getElement();
        Vertex person = edge.getVertex(Direction.IN, getAuthorizations());
        Vertex image = edge.getVertex(Direction.OUT, getAuthorizations());
        StreamingPropertyValue rawImage = BcSchema.RAW.getPropertyValue(image);

        try {
            // Execute extractor
            this.runExtractor(person, rawImage);

            // Mark edge as processed
            PROCESSED_PROPERTY.addPropertyValue(edge, "", Boolean.TRUE,
                    data.createPropertyMetadata(getUser()), getVisibilityTranslator().getDefaultVisibility(), getAuthorizations());
            getWebQueueRepository().broadcastPropertyChange(edge, "", PROCESSED_PROPERTY.getPropertyName(), data.getWorkspaceId());
            getWorkQueueRepository().pushGraphPropertyQueue(
                    edge,
                    "",
                    PROCESSED_PROPERTY.getPropertyName(),
                    data.getWorkspaceId(),
                    data.getVisibilitySource(),
                    Priority.HIGH,
                    ElementOrPropertyStatus.UPDATE,
                    null);

            getGraph().flush();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void runExtractor(Vertex vertex, StreamingPropertyValue rawImage) throws Exception {
        MultipartUtility multipart = new MultipartUtility(extractorUrl, "UTF-8");
        multipart.addFilePart("file", "file", rawImage.getInputStream());
        String descriptor = multipart.finish();
        if (!StringUtils.isEmpty(descriptor)) {
            Property noFaces = vertex.getProperty(FACE_NUMBER.getPropertyName());
            int numberOfFaces = 0;
            if (noFaces != null && noFaces.getValue() != null) {
                numberOfFaces = ((IntValue) noFaces.getValue()).value();
                FACE_NUMBER.removeProperty(vertex, "", getAuthorizations());
            }
            if (!USE_CLASSIFIER) {
                Property oldDescriptor = vertex.getProperty(FACE_DESCRIPTOR.getPropertyName());
                if (oldDescriptor != null && oldDescriptor.getValue() != null) {
                    final String old = ((TextValue) oldDescriptor.getValue()).stringValue();
                    descriptor = mean(old, descriptor, numberOfFaces);
                    FACE_DESCRIPTOR.removeProperty(vertex, old, getAuthorizations());
                }
            }
            FACE_DESCRIPTOR.addPropertyValue(vertex, descriptor, descriptor, vertex.getVisibility(), getAuthorizations());
            FACE_NUMBER.addPropertyValue(vertex, "", ++numberOfFaces, vertex.getVisibility(), getAuthorizations());

            getGraph().flush();

            getWebQueueRepository().broadcastPropertyChange(vertex, descriptor, FACE_DESCRIPTOR.getPropertyName(), null);
            getWorkQueueRepository().pushGraphPropertyQueue(
                    vertex,
                    descriptor,
                    FACE_DESCRIPTOR.getPropertyName(),
                    null,
                    null,
                    Priority.HIGH,
                    ElementOrPropertyStatus.UPDATE,
                    null
            );

            this.notifyDetectors();
        }
    }

    private void notifyDetectors() {
        this.detectorsUrls.forEach(url -> {
            try {
                final MultipartUtility req =
                        new MultipartUtility("http://" + url + DETECTOR_RELOAD_ENDPOINT, "UTF-8");
                req.finish();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    private String mean(String first, String second, int n) {
        String[] firstList = first.split(",");
        String[] secondList = second.split(",");
        StringBuffer result = new StringBuffer();
        for (int i = 0; i < firstList.length; i++) {
            if (!result.toString().isEmpty()) {
                result.append(",");
            }
            result.append(((Float.parseFloat(firstList[i]) * n + Float.parseFloat(secondList[i])) / (n + 1)) + "");
        }

        return result.toString();
    }
}
