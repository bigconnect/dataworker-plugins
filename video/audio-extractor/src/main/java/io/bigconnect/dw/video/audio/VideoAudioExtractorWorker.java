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
package io.bigconnect.dw.video.audio;

import com.google.inject.Inject;
import com.mware.core.ingest.dataworker.DataWorker;
import com.mware.core.ingest.dataworker.DataWorkerData;
import com.mware.core.ingest.dataworker.ElementOrPropertyStatus;
import com.mware.core.model.Description;
import com.mware.core.model.Name;
import com.mware.core.model.properties.BcSchema;
import com.mware.core.model.properties.MediaBcSchema;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.core.util.ProcessRunner;
import com.mware.ge.Element;
import com.mware.ge.Metadata;
import com.mware.ge.Property;
import com.mware.ge.Vertex;
import com.mware.ge.mutation.ExistingElementMutation;
import com.mware.ge.values.storable.ByteArray;
import com.mware.ge.values.storable.DefaultStreamingPropertyValue;
import com.mware.ge.values.storable.StreamingPropertyValue;
import com.mware.ge.values.storable.Values;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

@Name("Video Audio Extract")
@Description("Extracts the audio stream from a video")
public class VideoAudioExtractorWorker extends DataWorker {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(VideoAudioExtractorWorker.class);
    private static final String PROPERTY_KEY = "";
    private ProcessRunner processRunner;

    @Override
    public void execute(InputStream in, DataWorkerData data) throws Exception {
        File mp3File = File.createTempFile("audio_extract_", ".mp3");
        try {
            processRunner.execute(
                    "ffmpeg",
                    new String[]{
                            "-i", data.getLocalFile().getAbsolutePath(),
                            "-vn",
                            "-ar", "44100",
                            "-ab", "320k",
                            "-f", "mp3",
                            "-y",
                            mp3File.getAbsolutePath()
                    },
                    null,
                    data.getLocalFile().getAbsolutePath() + ": "
            );

            ExistingElementMutation<Vertex> m = refresh(data.getElement()).prepareMutation();

            try (InputStream mp3FileIn = new FileInputStream(mp3File)) {
                StreamingPropertyValue spv = new DefaultStreamingPropertyValue(mp3FileIn, ByteArray.class);
                spv.searchIndex(false);
                Metadata metadata = data.createPropertyMetadata(getUser());
                metadata.add(BcSchema.MIME_TYPE.getPropertyName(), Values.stringValue(MediaBcSchema.MIME_TYPE_AUDIO_MP3), getVisibilityTranslator().getDefaultVisibility());
                MediaBcSchema.AUDIO_MP3.setProperty(m, spv, metadata, data.getProperty().getVisibility());
                Element e = m.save(getAuthorizations());
                getGraph().flush();

                if (getWebQueueRepository().shouldBroadcast(data.getPriority())) {
                    getWebQueueRepository().broadcastPropertyChange(e, PROPERTY_KEY, MediaBcSchema.AUDIO_MP3.getPropertyName(), null);
                }
                getWorkQueueRepository().pushGraphPropertyQueue(
                        e,
                        PROPERTY_KEY,
                        MediaBcSchema.AUDIO_MP3.getPropertyName(),
                        null,
                        null,
                        data.getPriority(),
                        ElementOrPropertyStatus.UPDATE,
                        null
                );
            }
        } finally {
            if (!mp3File.delete()) {
                LOGGER.warn("Could not delete %s", mp3File.getAbsolutePath());
            }
        }
    }

    @Override
    public boolean isHandled(Element element, Property property) {
        if (property == null) {
            return false;
        }

        if (!property.getName().equals(BcSchema.RAW.getPropertyName())) {
            return false;
        }
        String mimeType = BcSchema.MIME_TYPE_METADATA.getMetadataValue(property.getMetadata(), null);
        if (mimeType == null || !mimeType.startsWith("video")) {
            return false;
        }

        if (MediaBcSchema.AUDIO_MP3.hasProperty(element)) {
            return false;
        }

        return true;
    }

    @Override
    public boolean isLocalFileRequired() {
        return true;
    }

    @Inject
    public void setProcessRunner(ProcessRunner ffmpeg) {
        this.processRunner = ffmpeg;
    }
}
