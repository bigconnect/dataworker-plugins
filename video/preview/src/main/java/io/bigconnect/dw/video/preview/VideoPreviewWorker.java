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
package io.bigconnect.dw.video.preview;

import com.google.inject.Inject;
import com.mware.bigconnect.ffmpeg.AVUtils;
import com.mware.bigconnect.ffmpeg.VideoFormat;
import com.mware.core.ingest.dataworker.DataWorker;
import com.mware.core.ingest.dataworker.DataWorkerData;
import com.mware.core.ingest.dataworker.DataWorkerPrepareData;
import com.mware.core.model.Description;
import com.mware.core.model.Name;
import com.mware.core.model.properties.BcSchema;
import com.mware.core.model.properties.MediaBcSchema;
import com.mware.core.model.properties.types.BcPropertyUpdate;
import com.mware.core.model.properties.types.DoubleBcProperty;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.core.util.ProcessRunner;
import com.mware.ge.*;
import com.mware.ge.collection.Pair;
import com.mware.ge.mutation.ExistingElementMutation;
import com.mware.ge.values.storable.ByteArray;
import com.mware.ge.values.storable.DefaultStreamingPropertyValue;
import com.mware.ge.values.storable.StreamingPropertyValue;
import com.mware.ge.values.storable.Values;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

@Name("Video Preview")
@Description("Gets a video preview by extracting a number of frames from the video")
public class VideoPreviewWorker extends DataWorker {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(VideoPreviewWorker.class);
    private static final String PROPERTY_KEY = VideoPreviewWorker.class.getName();
    private ProcessRunner processRunner;
    private DoubleBcProperty durationProperty;

    @Override
    public void prepare(DataWorkerPrepareData workerPrepareData) throws Exception {
        super.prepare(workerPrepareData);
        durationProperty = new DoubleBcProperty(getSchemaRepository().getRequiredPropertyNameByIntent("media.duration"));
    }

    @Override
    public void execute(InputStream in, DataWorkerData data) {
        Vertex element = (Vertex) refresh(data.getElement());

        Path videoFile = null;

        try {
            StreamingPropertyValue raw = BcSchema.RAW.getPropertyValue(element);
            if (raw == null)
                return;

            videoFile = Files.createTempFile("raw-", "");
            IOUtils.copy(raw.getInputStream(), new FileOutputStream(videoFile.toFile()));
            byte[] videoPosterFrame = AVUtils.createVideoPosterFrame(videoFile);
            if (videoPosterFrame.length == 0) {
                throw new RuntimeException("Poster frame not created.");
            }

            List<BcPropertyUpdate> changedProperties = new ArrayList<>();
            ExistingElementMutation<Vertex> m = element.prepareMutation();
            StreamingPropertyValue spv = new DefaultStreamingPropertyValue(new ByteArrayInputStream(videoPosterFrame), ByteArray.class, (long) videoPosterFrame.length);
            spv.searchIndex(false);
            Metadata metadata = data.createPropertyMetadata(getUser());
            metadata.add(BcSchema.MIME_TYPE.getPropertyName(), Values.stringValue("image/png"), getVisibilityTranslator().getDefaultVisibility());
            MediaBcSchema.RAW_POSTER_FRAME.updateProperty(changedProperties, element, m, PROPERTY_KEY, spv, metadata, data.getProperty().getVisibility());

            Pair<Integer, BufferedImage> videoPreviewImage = AVUtils.createVideoPreviewImage(videoFile);
            if (videoPreviewImage != null) {
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                ImageIO.write(videoPreviewImage.other(), "png", out);
                spv = new DefaultStreamingPropertyValue(new ByteArrayInputStream(out.toByteArray()), ByteArray.class);
                spv.searchIndex(false);
                metadata = data.createPropertyMetadata(getUser());
                MediaBcSchema.METADATA_VIDEO_PREVIEW_FRAMES.setMetadata(metadata, Long.valueOf(videoPreviewImage.first()), Visibility.EMPTY);
                MediaBcSchema.VIDEO_PREVIEW_IMAGE.updateProperty(changedProperties, element, m, spv, metadata, data.getProperty().getVisibility());
            }

            Element e = m.save(getAuthorizations());
            getGraph().flush();

            getWebQueueRepository().broadcastPropertiesChange(e, changedProperties, null, data.getPriority());

        } catch (IOException ex){
            LOGGER.warn("Could not create video preview image!", ex);
        } finally {
            if (videoFile != null)
                FileUtils.deleteQuietly(videoFile.toFile());
        }
    }

    @Override
    public boolean isHandled(Element element, Property property) {
        // we don't want the whole element
        if (property == null) {
            return false;
        }

        // we only want changes to the RAW property
        if (!property.getName().equals(MediaBcSchema.MEDIA_VIDEO_FORMAT.getPropertyName())) {
            return false;
        }

        // and only if the format is set
        String videoFormat = MediaBcSchema.MEDIA_VIDEO_FORMAT.getPropertyValue(element);
        if (!VideoFormat.MP4.name().equals(videoFormat) && !VideoFormat.WEBM.name().equals(videoFormat)) {
            return false;
        }

        return true;
    }

    @Inject
    public void setProcessRunner(ProcessRunner ffmpeg) {
        this.processRunner = ffmpeg;
    }
}
