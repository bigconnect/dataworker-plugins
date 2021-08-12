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
package io.bigconnect.dw.video.metadata;

import com.google.inject.Inject;
import com.mware.bigconnect.ffmpeg.AVMediaInfo;
import com.mware.core.ingest.dataworker.DataWorkerData;
import com.mware.core.ingest.dataworker.DataWorkerPrepareData;
import com.mware.core.ingest.dataworker.PostMimeTypeWorker;
import com.mware.core.model.Description;
import com.mware.core.model.Name;
import com.mware.core.model.properties.MediaBcSchema;
import com.mware.core.model.properties.types.*;
import com.mware.core.model.schema.SchemaRepository;
import com.mware.core.util.FileSizeUtil;
import com.mware.ge.Authorizations;
import com.mware.ge.Element;
import com.mware.ge.Metadata;
import com.mware.ge.Vertex;
import com.mware.ge.collection.Pair;
import com.mware.ge.mutation.ExistingElementMutation;
import net.bramp.ffmpeg.probe.FFmpegProbeResult;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

@Name("Video Metadata")
@Description("Extracts video metadata")
public class VideoMetadataPostMimeTypeWorker extends PostMimeTypeWorker {
    public static final String MULTI_VALUE_PROPERTY_KEY = VideoMetadataPostMimeTypeWorker.class.getName();
    private SchemaRepository schemaRepository;
    private DoubleBcProperty duration;
    private GeoPointBcProperty geoLocation;
    private DateTimeBcProperty dateTaken;
    private IntegerBcProperty width;
    private IntegerBcProperty height;
    private StringBcProperty mediaMetadata;
    private IntegerBcProperty fileSize;

    @Override
    public void prepare(DataWorkerPrepareData workerPrepareData) throws Exception {
        super.prepare(workerPrepareData);
        duration = new DoubleBcProperty(schemaRepository.getRequiredPropertyNameByIntent("media.duration"));
        geoLocation = new GeoPointBcProperty(schemaRepository.getRequiredPropertyNameByIntent("geoLocation"));
        dateTaken = new DateTimeBcProperty(schemaRepository.getRequiredPropertyNameByIntent("media.dateTaken"));
        width = new IntegerBcProperty(schemaRepository.getRequiredPropertyNameByIntent("media.width"));
        height = new IntegerBcProperty(schemaRepository.getRequiredPropertyNameByIntent("media.height"));
        mediaMetadata = new StringBcProperty(schemaRepository.getRequiredPropertyNameByIntent("media.metadata"));
        fileSize = new IntegerBcProperty(schemaRepository.getRequiredPropertyNameByIntent("media.fileSize"));
    }

    @Override
    public void execute(String mimeType, DataWorkerData data, Authorizations authorizations) throws Exception {
        if (!mimeType.startsWith("video")) {
            return;
        }
        File localFile = getLocalFileForRaw(data.getElement());
        FFmpegProbeResult videoMetadata = AVMediaInfo.probe(localFile.getAbsolutePath());
        ExistingElementMutation<Vertex> m = refresh(data.getElement(), authorizations).prepareMutation();
        List<BcPropertyUpdate> changedProperties = new ArrayList<>();
        Metadata metadata = data.createPropertyMetadata(getUser());
        if (videoMetadata != null) {
            if (AVMediaInfo.hasVideoStream(videoMetadata)) {
                MediaBcSchema.MEDIA_VIDEO_FORMAT.updateProperty(changedProperties, data.getElement(), m,
                        AVMediaInfo.getVideoFormat(videoMetadata).name(), metadata, data.getVisibility());
                MediaBcSchema.MEDIA_VIDEO_CODEC.updateProperty(changedProperties, data.getElement(), m,
                        AVMediaInfo.getVideoCodec(videoMetadata).name(), metadata, data.getVisibility());
            }

            if (AVMediaInfo.hasAudioStream(videoMetadata)) {
                MediaBcSchema.MEDIA_AUDIO_FORMAT.updateProperty(changedProperties, data.getElement(), m,
                        AVMediaInfo.getAudioFormat(videoMetadata).name(), metadata, data.getVisibility());
                MediaBcSchema.MEDIA_AUDIO_CODEC.updateProperty(changedProperties, data.getElement(), m,
                        AVMediaInfo.getAudioCodec(videoMetadata).name(), metadata, data.getVisibility());
            }

            setProperty(duration, videoMetadata.format.duration, m, metadata, data, changedProperties);
            setProperty(geoLocation, AVMediaInfo.getGeoPoint(videoMetadata), m, metadata, data, changedProperties);
            setProperty(dateTaken, AVMediaInfo.getDateTaken(videoMetadata), m, metadata, data, changedProperties);
            Pair<Integer, Integer> dimensions = AVMediaInfo.getDimensions(videoMetadata);
            if (dimensions != null) {
                setProperty(width, dimensions.first(), m, metadata, data, changedProperties);
                setProperty(height, dimensions.other(), m, metadata, data, changedProperties);
            }
            setProperty(this.mediaMetadata, videoMetadata.toString(), m, metadata, data, changedProperties);
        }

        setProperty(fileSize, FileSizeUtil.getSize(localFile), m, metadata, data, changedProperties);


        Element e = m.save(authorizations);
        getGraph().flush();

        getWebQueueRepository().broadcastPropertiesChange(e, changedProperties, null, data.getPriority());
        getWorkQueueRepository().pushGraphPropertyQueue(
                e,
                changedProperties,
                null,
                null,
                data.getPriority()
        );
    }

    private <T> void setProperty(BcProperty<T> property, T value, ExistingElementMutation<Vertex> mutation,
                                 Metadata metadata, DataWorkerData data,
                                 List<BcPropertyUpdate> changedProperties) {
        property.updateProperty(changedProperties, data.getElement(), mutation, MULTI_VALUE_PROPERTY_KEY, value,
                metadata, data.getVisibility());
    }

    @Inject
    public void setSchemaRepository(SchemaRepository ontologyRepository) {
        this.schemaRepository = ontologyRepository;
    }
}
