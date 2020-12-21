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
package io.bigconnect.dw.text.common;

import com.mware.core.model.properties.BcSchema;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.ge.Element;
import com.mware.ge.Property;
import com.mware.ge.collection.Iterables;
import org.apache.commons.lang3.StringUtils;

import java.util.Optional;

public class TextPropertyHelper {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(TextPropertyHelper.class);

    public static Optional<Property> getTextPropertyForLanguage(Element element, String language) {
        Iterable<Property> properties = BcSchema.TEXT.getProperties(element);

        Property property = Iterables.single(
                Iterables.filter(p -> {
                    if (p.getMetadata().containsKey(BcSchema.TEXT_LANGUAGE_METADATA.getMetadataKey())) {
                        String textLanguage = BcSchema.TEXT_LANGUAGE_METADATA.getMetadataValue(p);
                        return StringUtils.equals(language, textLanguage);
                    }
                    return false;
                }, properties)
        , null);

        Optional<Property> textProperty = Optional.ofNullable(property);
        if (!textProperty.isPresent()) {
            LOGGER.debug("Could not find a TEXT property with language: %s", language);
            return Optional.empty();
        }

        return textProperty;
    }

    public static String getTextLanguage(Property textProperty) {
        if (textProperty.getMetadata().containsKey(BcSchema.TEXT_LANGUAGE_METADATA.getMetadataKey())) {
            return BcSchema.TEXT_LANGUAGE_METADATA.getMetadataValue(textProperty);
        }

        return null;
    }
}
