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
package io.bigconnect.dw.video.frame;

import com.google.common.io.Files;
import com.google.inject.Inject;
import com.mware.bigconnect.ffmpeg.ArtifactThumbnailRepositoryProps;
import com.mware.core.ingest.dataworker.DataWorker;
import com.mware.core.ingest.dataworker.DataWorkerData;
import com.mware.core.ingest.dataworker.DataWorkerPrepareData;
import com.mware.core.ingest.dataworker.ElementOrPropertyStatus;
import com.mware.core.ingest.video.VideoFrameInfo;
import com.mware.core.model.Description;
import com.mware.core.model.Name;
import com.mware.core.model.properties.BcSchema;
import com.mware.core.model.properties.MediaBcSchema;
import com.mware.core.model.properties.types.DoubleBcProperty;
import com.mware.core.security.BcVisibility;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.core.util.ProcessRunner;
import com.mware.ge.*;
import com.mware.ge.mutation.ExistingElementMutation;
import com.mware.ge.util.StreamUtils;
import com.mware.ge.values.storable.*;
import org.apache.commons.io.FileUtils;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.mware.ge.util.IterableUtils.toList;

@Name("Video Frame Extract")
@Description("Extracts frames of the video for image processing")
public class VideoFrameExtractorWorker extends DataWorker {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(VideoFrameExtractorWorker.class);
    private ProcessRunner processRunner;
    private DoubleBcProperty videoDurationProperty;

    @Override
    public void prepare(DataWorkerPrepareData workerPrepareData) throws Exception {
        super.prepare(workerPrepareData);
        getGraphAuthorizationRepository().addAuthorizationToGraph(VideoFrameInfo.VISIBILITY_STRING);
        videoDurationProperty = new DoubleBcProperty(getSchemaRepository().getRequiredPropertyNameByIntent("media.duration"));
    }

    @Override
    public void execute(InputStream in, DataWorkerData data) throws Exception {
        Double videoDuration = videoDurationProperty.getOnlyPropertyValue(data.getElement());
        Visibility newVisibility = new BcVisibility(BcVisibility.and(getVisibilityTranslator().toVisibilityNoSuperUser(data.getVisibilityJson()), VideoFrameInfo.VISIBILITY_STRING)).getVisibility();

        Pattern fileNamePattern = Pattern.compile("image-([0-9]+)\\.png");
        File tempDir = Files.createTempDir();
        try {
            Double defaultFPSToExtract = 1.0;
            if (videoDuration != null && videoDuration <= ArtifactThumbnailRepositoryProps.FRAMES_PER_PREVIEW) {
                defaultFPSToExtract = (double)ArtifactThumbnailRepositoryProps.FRAMES_PER_PREVIEW / videoDuration;
            }
            extractFrames(data.getLocalFile(), tempDir, defaultFPSToExtract);

            ExistingElementMutation<Vertex> mutation = refresh(data.getElement()).prepareMutation();

            List<String> propertyKeys = new ArrayList<>();
            for (File frameFile : tempDir.listFiles()) {
                Matcher m = fileNamePattern.matcher(frameFile.getName());
                if (!m.matches()) {
                    continue;
                }
                long frameStartTime = (long) ((Double.parseDouble(m.group(1)) / defaultFPSToExtract) * 1000.0);

                try (InputStream frameFileIn = new FileInputStream(frameFile)) {
                    StreamingPropertyValue frameValue = new DefaultStreamingPropertyValue(frameFileIn, ByteArray.class);
                    frameValue.searchIndex(false);
                    String key = String.format("%08d", Math.max(0L, frameStartTime));

                    Metadata metadata = data.createPropertyMetadata(getUser());
                    metadata.add(BcSchema.MIME_TYPE.getPropertyName(), Values.stringValue("image/png"), getVisibilityTranslator().getDefaultVisibility());
                    metadata.add(MediaBcSchema.METADATA_VIDEO_FRAME_START_TIME, Values.longValue(frameStartTime), getVisibilityTranslator().getDefaultVisibility());

                    MediaBcSchema.VIDEO_FRAME.addPropertyValue(mutation, key, frameValue, metadata, newVisibility);
                    propertyKeys.add(key);
                }
            }

            generateAndSaveVideoPreviewImage(data, mutation);

            getGraph().flush();

            Element e = mutation.save(getAuthorizations());

            for (String propertyKey : propertyKeys) {
                if (getWebQueueRepository().shouldBroadcastGraphPropertyChange(MediaBcSchema.VIDEO_FRAME.getPropertyName(), data.getPriority())) {
                    getWebQueueRepository().broadcastPropertyChange(e, propertyKey, MediaBcSchema.VIDEO_FRAME.getPropertyName(), null);
                }

                getWorkQueueRepository().pushOnDwQueue(
                        e,
                        propertyKey,
                        MediaBcSchema.VIDEO_FRAME.getPropertyName(),
                        null,
                        null,
                        data.getPriority(),
                        ElementOrPropertyStatus.UPDATE,
                        null
                );
            }
        } finally {
            FileUtils.deleteDirectory(tempDir);
        }
    }

    private void extractFrames(File videoFileName, File outDir, double framesPerSecondToExtract) throws IOException, InterruptedException {
        String[] ffmpegOptionsArray = prepareFFMPEGOptions(videoFileName, outDir, framesPerSecondToExtract);
        processRunner.execute(
                "ffmpeg",
                ffmpegOptionsArray,
                null,
                videoFileName.getAbsolutePath() + ": "
        );
    }

    private String[] prepareFFMPEGOptions(File videoFileName, File outDir, double framesPerSecondToExtract) {

        ArrayList<String> ffmpegOptionsList = new ArrayList<>();
        ffmpegOptionsList.add("-i");
        ffmpegOptionsList.add(videoFileName.getAbsolutePath());
        ffmpegOptionsList.add("-r");
        ffmpegOptionsList.add("" + framesPerSecondToExtract);

        ffmpegOptionsList.add(new File(outDir, "image-%8d.png").getAbsolutePath());
        return ffmpegOptionsList.toArray(new String[ffmpegOptionsList.size()]);
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

        return true;
    }

    private void generateAndSaveVideoPreviewImage(DataWorkerData data, ExistingElementMutation<Vertex> artifactVertex) {
        LOGGER.info("Generating video preview for %s", artifactVertex.getElement().getId());

        try {
            Iterable<Property> videoFrames = getVideoFrameProperties(artifactVertex);
            List<Property> videoFramesForPreview = getFramesForPreview(videoFrames);
            BufferedImage previewImage = createPreviewImage(videoFramesForPreview);
            saveImage(data, artifactVertex, previewImage);
        } catch (IOException e) {
            throw new RuntimeException("Could not create preview image for artifact: " + artifactVertex.getElement().getId(), e);
        }

        LOGGER.debug("Finished creating preview for: %s", artifactVertex.getElement().getId());
    }

    private void saveImage(DataWorkerData data, ExistingElementMutation<Vertex> artifactVertex, BufferedImage previewImage) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ImageIO.write(previewImage, "png", out);
        StreamingPropertyValue spv = new DefaultStreamingPropertyValue(new ByteArrayInputStream(out.toByteArray()), ByteArray.class);
        spv.searchIndex(false);
        Metadata metadata = data.createPropertyMetadata(getUser());
        MediaBcSchema.VIDEO_PREVIEW_IMAGE.setProperty(artifactVertex, spv, metadata, artifactVertex.getNewElementVisibility());
        getGraph().flush();
    }

    private BufferedImage createPreviewImage(List<Property> videoFrames) throws IOException {
        int previewFrameWidth = ArtifactThumbnailRepositoryProps.PREVIEW_FRAME_HEIGHT;
        int previewFrameHeight = ArtifactThumbnailRepositoryProps.PREVIEW_FRAME_WIDTH;

        BufferedImage previewImage = null;
        Graphics g = null;
        for (int i = 0; i < videoFrames.size(); i++) {
            Property videoFrame = videoFrames.get(i);
            Image img = loadImage(videoFrame);
            int widthImage = img.getWidth(null);
            int heightImage = img.getHeight(null);
            if (i == 0) {
                float ratioImage = (float)widthImage / (float)heightImage;
                float ratioContainer = (float)previewFrameWidth / (float)previewFrameHeight;
                float calculatedWidth, calculatedHeight;
                if (ratioContainer > ratioImage) {
                    calculatedWidth = widthImage * ((float)previewFrameHeight / heightImage);
                    calculatedHeight = (float)previewFrameHeight;
                } else {
                    calculatedWidth = (float)previewFrameWidth;
                    calculatedHeight = heightImage * ((float)previewFrameWidth / widthImage);
                }
                previewFrameWidth = (int) calculatedWidth;
                previewFrameHeight = (int) calculatedHeight;
                previewImage = new BufferedImage(previewFrameWidth * videoFrames.size(), previewFrameHeight, BufferedImage.TYPE_INT_RGB);
                g = previewImage.getGraphics();
            }
            int dx1 = i * previewFrameWidth;
            int dy1 = 0;
            int dx2 = dx1 + previewFrameWidth;
            int dy2 = previewFrameHeight;
            int sx1 = 0;
            int sy1 = 0;
            g.drawImage(img, dx1, dy1, dx2, dy2, sx1, sy1, widthImage, heightImage, null);
        }
        return previewImage;
    }

    private Image loadImage(Property videoFrame) throws IOException {
        StreamingPropertyValue spv = (StreamingPropertyValue) videoFrame.getValue();
        try (InputStream spvIn = spv.getInputStream()) {
            BufferedImage img = ImageIO.read(spvIn);
            checkNotNull(img, "Could not load image from frame: " + videoFrame);
            return img;
        }
    }

    private Iterable<Property> getVideoFrameProperties(ExistingElementMutation<Vertex> artifactVertex) {
        return StreamUtils.stream(artifactVertex.getProperties())
                .filter(p -> p.getName().equals(MediaBcSchema.VIDEO_FRAME.getPropertyName()))
                .sorted((p1, p2) -> {
                    LongValue p1StartTime = (LongValue) p1.getMetadata().getValue(MediaBcSchema.METADATA_VIDEO_FRAME_START_TIME);
                    LongValue p2StartTime = (LongValue) p2.getMetadata().getValue(MediaBcSchema.METADATA_VIDEO_FRAME_START_TIME);
                    return p1StartTime.compareTo(p2StartTime);
                })
                .collect(Collectors.toList());
    }

    private List<Property> getFramesForPreview(Iterable<Property> videoFramesIterable) {
        List<Property> videoFrames = toList(videoFramesIterable);
        ArrayList<Property> results = new ArrayList<>();
        double skip = (double) videoFrames.size() / (double) ArtifactThumbnailRepositoryProps.FRAMES_PER_PREVIEW;
        for (double i = 0; i < videoFrames.size(); i += skip) {
            results.add(videoFrames.get((int) Math.floor(i)));
        }
        if (results.size() < ArtifactThumbnailRepositoryProps.FRAMES_PER_PREVIEW) {
            results.add(videoFrames.get(videoFrames.size() - 1));
        }
        if (results.size() > ArtifactThumbnailRepositoryProps.FRAMES_PER_PREVIEW) {
            results.remove(results.size() - 1);
        }
        return results;
    }

    @Inject
    public void setProcessRunner(ProcessRunner processRunner) {
        this.processRunner = processRunner;
    }
}
