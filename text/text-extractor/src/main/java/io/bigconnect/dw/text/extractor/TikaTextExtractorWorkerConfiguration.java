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
package io.bigconnect.dw.text.extractor;

import com.google.inject.Inject;
import com.mware.core.config.Configurable;
import com.mware.core.config.Configuration;
import com.mware.core.model.properties.BcSchema;
import com.mware.ge.Element;
import com.mware.ge.Property;

import java.util.Map;

public class TikaTextExtractorWorkerConfiguration {
    public static final String CONFIGURATION_PREFIX = TikaTextExtractorWorker.class.getName();
    public static final String TEXT_EXTRACT_MAPPING_CONFIGURATION_PREFIX = CONFIGURATION_PREFIX + ".textExtractMapping";
    public static final String DEFAULT_TEXT_EXTRACT_MAPPING = "raw";

    private final Map<String, TextExtractMapping> textExtractMappings;

    @Inject
    public TikaTextExtractorWorkerConfiguration(Configuration configuration) {
        textExtractMappings = configuration.getMultiValueConfigurables(TEXT_EXTRACT_MAPPING_CONFIGURATION_PREFIX, TextExtractMapping.class);

        if (!textExtractMappings.containsKey(DEFAULT_TEXT_EXTRACT_MAPPING)) {
            TextExtractMapping textExtractMapping = new TextExtractMapping();
            textExtractMapping.rawPropertyName = BcSchema.MIME_TYPE.getPropertyName();
            textExtractMapping.extractedTextPropertyName = BcSchema.TEXT.getPropertyName();
            textExtractMapping.textDescription = "Text";
            textExtractMappings.put(DEFAULT_TEXT_EXTRACT_MAPPING, textExtractMapping);
        }
    }

    boolean isHandled(Element element, Property property) {
        for (TextExtractMapping textExtractMapping : this.textExtractMappings.values()) {
            if (textExtractMapping.rawPropertyName.equals(property.getName()) &&
                    element.getProperty(textExtractMapping.extractedTextPropertyName) == null) {
                return true;
            }
        }
        return false;
    }

    TextExtractMapping getTextExtractMapping(Property property) {
        for (TextExtractMapping textExtractMapping : this.textExtractMappings.values()) {
            if (textExtractMapping.rawPropertyName.equals(property.getName())) {
                return textExtractMapping;
            }
        }
        return null;
    }

    public static class TextExtractMapping {
        @Configurable()
        private String rawPropertyName;

        @Configurable()
        private String extractedTextPropertyName;

        @Configurable(required = false)
        private String textDescription;

        public String getRawPropertyName() {
            return rawPropertyName;
        }

        public String getExtractedTextPropertyName() {
            return extractedTextPropertyName;
        }

        public String getTextDescription() {
            return textDescription;
        }
    }
}
