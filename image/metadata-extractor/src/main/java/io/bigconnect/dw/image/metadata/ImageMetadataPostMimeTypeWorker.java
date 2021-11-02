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
package io.bigconnect.dw.image.metadata;

import com.drew.imaging.FileType;
import com.drew.imaging.FileTypeDetector;
import com.drew.imaging.ImageMetadataReader;
import com.drew.metadata.Metadata;
import com.mware.core.ingest.dataworker.*;
import com.mware.core.model.Description;
import com.mware.core.model.Name;
import com.mware.core.model.schema.SchemaRepository;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.core.util.FileSizeUtil;
import com.mware.ge.Authorizations;
import com.mware.ge.Element;
import com.mware.ge.Vertex;
import com.mware.ge.mutation.ExistingElementMutation;
import com.mware.ge.values.storable.Value;
import com.mware.ge.values.storable.Values;
import io.bigconnect.dw.image.metadata.utils.*;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;

@Name("Drewnoakes Image Metadata")
@Description("Extracts image metadata")
public class ImageMetadataPostMimeTypeWorker extends PostMimeTypeWorker {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(ImageMetadataPostMimeTypeWorker.class);
    public static final String MULTI_VALUE_KEY = ImageMetadataPostMimeTypeWorker.class.getName();
    private String fileSizeIri;
    private String dateTakenIri;
    private String deviceMakeIri;
    private String deviceModelIri;
    private String geoLocationIri;
    private String headingIri;
    private String metadataIri;
    private String widthIri;
    private String heightIri;

    @Override
    public void prepare(DataWorkerPrepareData workerPrepareData) throws Exception {
        super.prepare(workerPrepareData);
        headingIri = getSchemaRepository().getRequiredPropertyNameByIntent("media.imageHeading", SchemaRepository.PUBLIC);
        geoLocationIri = getSchemaRepository().getRequiredPropertyNameByIntent("geoLocation", SchemaRepository.PUBLIC);
        dateTakenIri = getSchemaRepository().getRequiredPropertyNameByIntent("media.dateTaken", SchemaRepository.PUBLIC);
        deviceMakeIri = getSchemaRepository().getRequiredPropertyNameByIntent("media.deviceMake", SchemaRepository.PUBLIC);
        deviceModelIri = getSchemaRepository().getRequiredPropertyNameByIntent("media.deviceModel", SchemaRepository.PUBLIC);
        widthIri = getSchemaRepository().getRequiredPropertyNameByIntent("media.width", SchemaRepository.PUBLIC);
        heightIri = getSchemaRepository().getRequiredPropertyNameByIntent("media.height", SchemaRepository.PUBLIC);
        metadataIri = getSchemaRepository().getRequiredPropertyNameByIntent("media.metadata", SchemaRepository.PUBLIC);
        fileSizeIri = getSchemaRepository().getRequiredPropertyNameByIntent("media.fileSize", SchemaRepository.PUBLIC);
    }

    private void setProperty(String iri, Value value, ExistingElementMutation<Vertex> mutation, com.mware.ge.Metadata metadata, DataWorkerData data, List<String> properties) {
        if (iri != null && value != null) {
            mutation.addPropertyValue(MULTI_VALUE_KEY, iri, value, metadata, data.getVisibility());
            properties.add(iri);
        }
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

        BufferedInputStream fileInputStream = new BufferedInputStream(new FileInputStream(imageFile));
        FileType detectedFileType = FileTypeDetector.detectFileType(fileInputStream);
        if(detectedFileType == null || detectedFileType == FileType.Unknown) {
            return;
        }

        com.mware.ge.Metadata metadata = data.createPropertyMetadata(getUser());
        ExistingElementMutation<Vertex> mutation = refresh(data.getElement(), authorizations).prepareMutation();
        List<String> properties = new ArrayList<>();

        Metadata imageMetadata = null;
        try {
            imageMetadata = ImageMetadataReader.readMetadata(fileInputStream);
        } catch (Exception e) {
            LOGGER.error("Could not read metadata from imageFile: %s", imageFile, e);
        }

        Integer width = null;
        Integer height = null;
        if (imageMetadata != null) {
            setProperty(dateTakenIri, DateExtractor.getDateDefault(imageMetadata), mutation, metadata, data, properties);
            setProperty(deviceMakeIri, MakeExtractor.getMake(imageMetadata), mutation, metadata, data, properties);
            setProperty(deviceModelIri, ModelExtractor.getModel(imageMetadata), mutation, metadata, data, properties);
            setProperty(geoLocationIri, GeoPointExtractor.getGeoPoint(imageMetadata), mutation, metadata, data, properties);
            setProperty(headingIri, HeadingExtractor.getImageHeading(imageMetadata), mutation, metadata, data, properties);

            width = DimensionsExtractor.getWidthViaMetadata(imageMetadata);
            height = DimensionsExtractor.getHeightViaMetadata(imageMetadata);
        }

        if(width == null) {
            width = DimensionsExtractor.getWidthViaBufferedImage(imageFile);
        }
        setProperty(widthIri, Values.of(width), mutation, metadata, data, properties);

        if(height == null) {
            height = DimensionsExtractor.getHeightViaBufferedImage(imageFile);
        }
        setProperty(heightIri, Values.of(height), mutation, metadata, data, properties);

        setProperty(fileSizeIri, Values.of(FileSizeUtil.getSize(imageFile)), mutation, metadata, data, properties);

        Element e = mutation.save(authorizations);
        getGraph().flush();

        for (String propertyName : properties) {
            if (getWebQueueRepository().shouldBroadcastGraphPropertyChange(propertyName, data.getPriority())) {
                getWebQueueRepository().broadcastPropertyChange(e, MULTI_VALUE_KEY, propertyName, null);
            }

            getWorkQueueRepository().pushOnDwQueue(
                    e,
                    MULTI_VALUE_KEY,
                    propertyName,
                    null,
                    null,
                    data.getPriority(),
                    ElementOrPropertyStatus.UPDATE,
                    null);
        }
    }
}
