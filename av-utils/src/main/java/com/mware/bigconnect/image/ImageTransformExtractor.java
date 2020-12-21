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
package com.mware.bigconnect.image;

import com.drew.imaging.ImageMetadataReader;
import com.drew.imaging.ImageProcessingException;
import com.drew.metadata.Metadata;
import com.drew.metadata.exif.ExifIFD0Directory;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;

import java.io.*;

public class ImageTransformExtractor {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(ImageTransformExtractor.class);

    public static ImageTransform getImageTransform(byte[] data) {
        return getImageTransform(new ByteArrayInputStream(data));
    }

    public static ImageTransform getImageTransform(InputStream inputStream) {
        try {
            //Attempt to retrieve the metadata from the image.
            BufferedInputStream in = new BufferedInputStream(inputStream);
            Metadata metadata = ImageMetadataReader.readMetadata(in);
            return getImageTransformFromMetadata(metadata);
        } catch (ImageProcessingException e) {
            LOGGER.error("drewnoakes metadata extractor threw ImageProcessingException when reading metadata." +
                    " Returning default orientation for image.", e);
        } catch (IOException e) {
            LOGGER.error("drewnoakes metadata extractor threw IOException when reading metadata." +
                    " Returning default orientation for image.", e);
        }

        return getNoFlipNoRotationImageTransform();
    }

    public static ImageTransform getImageTransform(File localFile) {
        try {
            //Attempt to retrieve the metadata from the image.
            Metadata metadata = ImageMetadataReader.readMetadata(localFile);
            return getImageTransformFromMetadata(metadata);
        } catch (ImageProcessingException e) {
            LOGGER.error("drewnoakes metadata extractor threw ImageProcessingException when reading metadata." +
                    " Returning default orientation for image.", e);
        } catch (IOException e) {
            LOGGER.error("drewnoakes metadata extractor threw IOException when reading metadata." +
                    " Returning default orientation for image.", e);
        }

        return getNoFlipNoRotationImageTransform();
    }

    private static ImageTransform getImageTransformFromMetadata(Metadata metadata) {
        //new ImageTransform(false, 0) is the original image orientation, with no flip needed, and no rotation needed.
        ImageTransform imageTransform = getNoFlipNoRotationImageTransform();

        if (metadata != null) {
            ExifIFD0Directory directory = metadata.getFirstDirectoryOfType(ExifIFD0Directory.class);
            if (directory != null) {
                Integer orientationInteger = directory.getInteger(ExifIFD0Directory.TAG_ORIENTATION);
                if (orientationInteger != null) {
                    imageTransform = convertOrientationToTransform(orientationInteger);
                }
            }
        }

        return imageTransform;
    }

    private static ImageTransform getNoFlipNoRotationImageTransform() {
        return new ImageTransform(false, 0);
    }

    /**
     * Converts an orientation number to an ImageTransform object used by bigCONNECT.
     *
     * @param orientationInt The EXIF orientation number, from 1 - 8, representing the combinations of 4 different
     *                       rotations and 2 different flipped values.
     */
    public static ImageTransform convertOrientationToTransform(int orientationInt) {
        switch (orientationInt) {
            case 1:
                return getNoFlipNoRotationImageTransform();
            case 2:
                return new ImageTransform(true, 0);
            case 3:
                return new ImageTransform(false, 180);
            case 4:
                return new ImageTransform(true, 180);
            case 5:
                return new ImageTransform(true, 270);
            case 6:
                return new ImageTransform(false, 90);
            case 7:
                return new ImageTransform(true, 90);
            case 8:
                return new ImageTransform(false, 270);
            default:
                return getNoFlipNoRotationImageTransform();
        }
    }
}
