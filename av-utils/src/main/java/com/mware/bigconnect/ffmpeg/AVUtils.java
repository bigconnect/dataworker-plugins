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
package com.mware.bigconnect.ffmpeg;

import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.ge.collection.Pair;
import net.bramp.ffmpeg.FFmpeg;
import net.bramp.ffmpeg.FFmpegExecutor;
import net.bramp.ffmpeg.FFprobe;
import net.bramp.ffmpeg.ProcessFunction;
import net.bramp.ffmpeg.builder.FFmpegBuilder;
import net.bramp.ffmpeg.builder.FFmpegOutputBuilder;
import net.bramp.ffmpeg.probe.FFmpegProbeResult;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.mware.bigconnect.ffmpeg.ArtifactThumbnailRepositoryProps.*;

public class AVUtils {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(AVUtils.class);

    public static FFprobe ffprobe() throws IOException {
        String path = System.getenv("FFPROBE");
        return (!StringUtils.isEmpty(path)) ? new FFprobe(path) : new FFprobe();
    }

    public static FFmpeg ffmpeg() throws IOException {
        String path = System.getenv("FFMPEG");
        return (!StringUtils.isEmpty(path)) ? new FFmpeg(path) : new FFmpeg();
    }

    public static FFmpeg ffmpeg(ProcessFunction runFunction) throws IOException {
        String path = System.getenv("FFMPEG");
        return (!StringUtils.isEmpty(path)) ? new FFmpeg(path, runFunction) : new FFmpeg(runFunction);
    }

    public static byte[] createVideoPosterFrame(Path videoFile) {
        FFmpegProbeResult probeResult = AVMediaInfo.probe(videoFile.toString());
        if (probeResult == null)
            return new byte[0];

        double duration = probeResult.format.duration;
        File destFile = null;

        try {
            destFile = Files.createTempFile("poster-", ".png").toFile();

            FFmpegOutputBuilder output = new FFmpegOutputBuilder()
                    .setVideoCodec("png")
                    .setFrames(1)
                    .disableAudio()
                    .setFilename(destFile.getAbsolutePath());

            FFmpegBuilder builder = AVUtils.ffmpeg().builder()
                    .setInput(probeResult)
                    .addOutput(output)
                    .overrideOutputFiles(true)
                    .addExtraArgs("-itsoffset").addExtraArgs("-" + (duration / 3.0));

            FFmpegExecutor executor = new FFmpegExecutor(AVUtils.ffmpeg(), AVUtils.ffprobe());
            executor.createJob(builder).run();

            if (destFile.exists()) {
                return Files.readAllBytes(destFile.toPath());
            } else {
                LOGGER.warn("Failed to extract poster frame for an unknown reason.");
                return new byte[0];
            }
        } catch (IOException e) {
            LOGGER.warn("Could not create poster frame for video file: " + videoFile);
        } finally {
            FileUtils.deleteQuietly(destFile);
        }

        return new byte[0];
    }

    // creates a very wide PNG image with frames extracted from the video
    public static Pair<Integer, BufferedImage> createVideoPreviewImage(Path videoFileName) {
        File tempDir = null;
        try {
            FFmpegProbeResult probeResult = AVMediaInfo.probe(videoFileName.toFile().getAbsolutePath());
            if (probeResult == null)
                return null;

            double videoDuration = probeResult.format.duration;
            tempDir = Files.createTempDirectory("frames-").toFile();

            int framesPerMinute = videoDuration < 60 ? 1 : FRAMES_PER_MINUTE;
            String extractionRule = videoDuration > 60 ? ""+FRAMES_PER_MINUTE+"/"+60 : "1/"+videoDuration;

            FFmpegOutputBuilder output = new FFmpegOutputBuilder()
                    .setVideoFilter("fps=fps="+extractionRule)
                    .setFilename(new File(tempDir, "image-%8d.png").getAbsolutePath());

            FFmpegBuilder builder = AVUtils.ffmpeg().builder()
                    .setInput(videoFileName.toFile().getAbsolutePath())
                    .addOutput(output);

            FFmpegExecutor executor = new FFmpegExecutor(AVUtils.ffmpeg(), AVUtils.ffprobe());
            executor.createJob(builder).run();

            Pattern fileNamePattern = Pattern.compile("image-([0-9]+)\\.png");
            java.util.List<Pair<Long, File>> videoFrames = new ArrayList<>();

            for (File frameFile : tempDir.listFiles()) {
                Matcher m = fileNamePattern.matcher(frameFile.getName());
                if (!m.matches()) {
                    continue;
                }
                long frameStartTime = Long.parseLong(m.group(1)) * framesPerMinute;
                videoFrames.add(Pair.of(frameStartTime, frameFile));
            }

            videoFrames.sort(Comparator.comparingLong(Pair::first));

            int previewFrameWidth = PREVIEW_FRAME_WIDTH;
            int previewFrameHeight = PREVIEW_FRAME_HEIGHT;

            BufferedImage previewImage = null;
            Graphics g = null;
            for (int i = 0; i < videoFrames.size(); i++) {
                Pair<Long, File> videoFrame = videoFrames.get(i);
                BufferedImage img = loadImage(videoFrame.other());
                int widthImage = img.getWidth(null);
                int heightImage = img.getHeight(null);
                if (i == 0) {
                    float ratioImage = (float) widthImage / (float) heightImage;
                    float ratioContainer = (float) previewFrameWidth / (float) previewFrameHeight;
                    float calculatedWidth, calculatedHeight;
                    if (ratioContainer > ratioImage) {
                        calculatedWidth = widthImage * ((float) previewFrameHeight / heightImage);
                        calculatedHeight = (float) previewFrameHeight;
                    } else {
                        calculatedWidth = (float) previewFrameWidth;
                        calculatedHeight = heightImage * ((float) previewFrameWidth / widthImage);
                    }
                    previewFrameWidth = (int) calculatedWidth;
                    previewFrameHeight = (int) calculatedHeight;
                    previewImage = new BufferedImage(previewFrameWidth * videoFrames.size(), previewFrameHeight, BufferedImage.TYPE_INT_RGB);
                    g = previewImage.createGraphics();
                }
                int dx1 = i * previewFrameWidth;
                int dy1 = 0;
                int dx2 = dx1 + previewFrameWidth;
                int dy2 = previewFrameHeight;
                int sx1 = 0;
                int sy1 = 0;
                g.drawImage(img, dx1, dy1, dx2, dy2, sx1, sy1, widthImage, heightImage, null);
            }

            if (previewImage != null) {
                g.dispose();
                return Pair.of(videoFrames.size(), previewImage);
            }

        } catch (IOException ex) {
            LOGGER.warn("Could not generate video preview.", ex);
        } finally {
            try {
                FileUtils.deleteDirectory(tempDir);
            } catch (IOException e) {
            }
        }

        return null;
    }

    private static BufferedImage loadImage(File videoFrame) throws IOException {
        try (InputStream spvIn = new FileInputStream(videoFrame)) {
            BufferedImage img = ImageIO.read(spvIn);
            checkNotNull(img, "Could not load image from frame: " + videoFrame);
            return img;
        }
    }
}
