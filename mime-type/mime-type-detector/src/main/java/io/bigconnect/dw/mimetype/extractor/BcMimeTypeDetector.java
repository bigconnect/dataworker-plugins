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
package io.bigconnect.dw.mimetype.extractor;

import com.mware.core.exception.BcException;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import org.apache.commons.io.FilenameUtils;
import org.apache.tika.detect.DefaultDetector;
import org.apache.tika.detect.Detector;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URLConnection;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BcMimeTypeDetector implements Detector {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(BcMimeTypeDetector.class);
    public static final String EXT_TO_MIME_TYPE_MAPPING_FILE = "extToMimeTypeMapping.txt";
    public static final String METADATA_FILENAME = "fileName";
    private final DefaultDetector defaultDetector;
    private static final Map<String, String> extToMimeTypeMapping = loadExtToMimeTypeMappingFile();

    public BcMimeTypeDetector() {
        defaultDetector = new DefaultDetector();
    }

    @Override
    public MediaType detect(InputStream input, Metadata metadata) throws IOException {
        String fileName = metadata.get(METADATA_FILENAME);
        if (fileName != null) {
            String mimeType = URLConnection.guessContentTypeFromName(fileName);
            if (mimeType != null) {
                return toMediaType(mimeType);
            }

            MediaType mediaType = setContentTypeUsingFileExt(FilenameUtils.getExtension(fileName).toLowerCase());
            if (mediaType != null) {
                return mediaType;
            }
        }

        return defaultDetector.detect(TikaInputStream.get(input), metadata);
    }

    private static Map<String, String> loadExtToMimeTypeMappingFile() {
        Map<String, String> results = new HashMap<>();
        try {
            InputStream in = BcMimeTypeDetector.class.getResourceAsStream(EXT_TO_MIME_TYPE_MAPPING_FILE);
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            Pattern linePattern = Pattern.compile("(.+)\\s+(.+)");
            String line;
            while ((line = reader.readLine()) != null) {
                Matcher m = linePattern.matcher(line);
                if (!m.matches()) {
                    LOGGER.warn("Invalid line in mime type mapping file: %s", line);
                    continue;
                }
                String ext = m.group(1).trim().toLowerCase();
                String mimeType = m.group(2).trim();
                if (ext.startsWith(".")) {
                    ext = ext.substring(1);
                }

                // take the first entry because the second entry is the alternative mime type
                if (!results.containsKey(ext)) {
                    results.put(ext, mimeType);
                }
            }
            in.close();
        } catch (IOException ex) {
            throw new BcException("Could not load " + EXT_TO_MIME_TYPE_MAPPING_FILE);
        }
        return results;
    }

    private MediaType setContentTypeUsingFileExt(String fileExt) {
        if (extToMimeTypeMapping.containsKey(fileExt)) {
            return toMediaType(extToMimeTypeMapping.get(fileExt));
        }
        return null;
    }

    private MediaType toMediaType(String str) {
        String[] parts = str.split("/");
        return new MediaType(parts[0], parts[1]);
    }
}
