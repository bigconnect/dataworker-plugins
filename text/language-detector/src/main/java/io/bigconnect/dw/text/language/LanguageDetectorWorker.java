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
package io.bigconnect.dw.text.language;

import com.google.common.base.Optional;
import com.mware.core.ingest.dataworker.DataWorker;
import com.mware.core.ingest.dataworker.DataWorkerData;
import com.mware.core.ingest.dataworker.DataWorkerPrepareData;
import com.mware.core.ingest.dataworker.ElementOrPropertyStatus;
import com.mware.core.model.Description;
import com.mware.core.model.Name;
import com.mware.core.model.properties.BcSchema;
import com.mware.core.model.properties.RawObjectSchema;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.ge.Element;
import com.mware.ge.Property;
import com.mware.ge.Vertex;
import com.mware.ge.Visibility;
import com.mware.ge.mutation.ExistingElementMutation;
import com.mware.ge.values.storable.Values;
import com.optimaize.langdetect.i18n.LdLocale;
import com.optimaize.langdetect.profiles.BuiltInLanguages;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Name("Text Language Detector")
@Description("Detect the language of a piece of text")
public class LanguageDetectorWorker extends DataWorker {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(LanguageDetectorWorker.class);
    private LanguageDetectorUtil languageDetector;

    @Override
    public void prepare(DataWorkerPrepareData workerPrepareData) throws Exception {
        super.prepare(workerPrepareData);
        this.languageDetector = new LanguageDetectorUtil();
    }

    @Override
    public boolean isHandled(Element element, Property property) {
        if (property == null) {
            return false;
        }

        // if the text was changed
        return BcSchema.TEXT.getPropertyName().equals(property.getName());
    }

    @Override
    public void execute(InputStream in, DataWorkerData data) throws Exception {
        String text = IOUtils.toString(in, StandardCharsets.UTF_8);
        if (StringUtils.isEmpty(text)) {
            return;
        }

        Optional<String> language = languageDetector.detectLanguage(text);
        if (language.isPresent()) {
            // set the new language
            ExistingElementMutation<Vertex> m = refresh(data.getElement()).prepareMutation();
            m.setPropertyMetadata(data.getProperty(), BcSchema.TEXT_LANGUAGE_METADATA.getMetadataKey(),
                    Values.stringValue(language.get()), Visibility.EMPTY);

            // the key for the RAW_LANGUAGE prop must be the same as the TEXT prop key for which the language is detected
            // this combination is used later in the EntityExtractor and SentimentExtractor
            m.addPropertyValue(data.getProperty().getKey(), RawObjectSchema.RAW_LANGUAGE.getPropertyName(), Values.stringValue(language.get()), Visibility.EMPTY);

            Element e = m.save(getAuthorizations());
            getGraph().flush();

            getWorkQueueRepository().pushGraphPropertyQueue(
                    e,
                    data.getProperty().getKey(),
                    RawObjectSchema.RAW_LANGUAGE.getPropertyName(),
                    data.getWorkspaceId(),
                    null,
                    data.getPriority(),
                    ElementOrPropertyStatus.UPDATE,
                    null);
        } else {
            LOGGER.warn("Could not detect language for text: " + text);
        }
    }

    public static Set<String> getSupportedLanguages() {
        List<LdLocale> languages = BuiltInLanguages.getLanguages();
        return languages.stream()
                .map(LdLocale::getLanguage)
                .collect(Collectors.toSet());
    }
}
