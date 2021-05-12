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
package io.bigconnect.dw.google.translate;

import com.google.cloud.translate.v3beta1.*;
import com.google.common.base.Optional;
import com.google.inject.Singleton;
import com.mware.core.ingest.dataworker.DataWorker;
import com.mware.core.ingest.dataworker.DataWorkerData;
import com.mware.core.ingest.dataworker.DataWorkerPrepareData;
import com.mware.core.ingest.dataworker.ElementOrPropertyStatus;
import com.mware.core.model.Description;
import com.mware.core.model.Name;
import com.mware.core.model.clientapi.dto.VisibilityJson;
import com.mware.core.model.properties.BcSchema;
import com.mware.core.model.properties.RawObjectSchema;
import com.mware.core.model.properties.types.BooleanBcProperty;
import com.mware.core.model.properties.types.PropertyMetadata;
import com.mware.core.model.workQueue.Priority;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.ge.Element;
import com.mware.ge.Metadata;
import com.mware.ge.Property;
import com.mware.ge.Visibility;
import com.mware.ge.util.Preconditions;
import com.mware.ge.values.storable.BooleanValue;
import com.mware.ge.values.storable.DefaultStreamingPropertyValue;
import com.mware.ge.values.storable.StreamingPropertyValue;
import io.bigconnect.dw.google.common.schema.GoogleCredentialUtils;
import io.bigconnect.dw.text.common.TextPropertyHelper;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static io.bigconnect.dw.google.translate.GoogleTranslateSchemaContribution.GOOGLE_TRANSLATE_PROPERTY;

@Name("Google Translate")
@Description("Uses Google API to translate text to English")
@Singleton
public class GoogleTranslateDataWorker extends DataWorker {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(GoogleTranslateDataWorker.class);
    private static final String CONFIG_TARGET_LANGUAGE = "google.translate.target_language";

    private Set<String> supportedLanguages;
    private String targetLanguage;
    private LocationName locationName;
    private LanguageDetectorUtil languageDetector;

    @Override
    public void prepare(DataWorkerPrepareData workerPrepareData) throws Exception {
        super.prepare(workerPrepareData);
        GoogleCredentialUtils.checkCredentials();

        targetLanguage = getConfiguration().get(CONFIG_TARGET_LANGUAGE, "");
        Preconditions.checkState(!StringUtils.isEmpty(targetLanguage),
                "Please provide the " + CONFIG_TARGET_LANGUAGE + " config property");

        supportedLanguages = getSupportedTranslations();
        Preconditions.checkState(supportedLanguages != null && !supportedLanguages.isEmpty(),
                "No translations supported for language: " + targetLanguage);

        locationName = LocationName.of(GoogleCredentialUtils.getProjectId(), "global");
        this.languageDetector = new LanguageDetectorUtil();
    }

    @Override
    public boolean isHandled(Element element, Property property) {
        if (property == null) {
            return false;
        }

        if (GOOGLE_TRANSLATE_PROPERTY.getPropertyName().equals(property.getName())) {
            Boolean performTranslate = GOOGLE_TRANSLATE_PROPERTY.getPropertyValue(element, false);
            return Boolean.TRUE.equals(performTranslate);
        }

        return false;
    }

    @Override
    public void execute(InputStream in, DataWorkerData data) throws Exception {
        LOGGER.info("Preparing to translate...");
        Element element = refresh(data.getElement());

        List<Property> propertiesToTranslate = new ArrayList<>();
        // find first property different than target language
        for (Property property : BcSchema.TEXT.getProperties(element)) {
            String textLanguage = TextPropertyHelper.getTextLanguage(property);
            if (StringUtils.isEmpty(textLanguage)) {
                StreamingPropertyValue textSpv = BcSchema.TEXT.getPropertyValue(property);
                if (textSpv != null) {
                    textLanguage = languageDetector.detectLanguage(textSpv.readToString()).or("");
                }
            }
            if (StringUtils.isEmpty(textLanguage) || !targetLanguage.equals(textLanguage)) {
                boolean canTranslate = StringUtils.isEmpty(textLanguage) || supportedLanguages.contains(textLanguage);
                if (canTranslate)
                    propertiesToTranslate.add(property);
            }
        }

        GOOGLE_TRANSLATE_PROPERTY.setProperty(element, Boolean.FALSE, Visibility.EMPTY, element.getAuthorizations());

        try (TranslationServiceClient googleClient = TranslationServiceClient.create()) {
            for (Property property : propertiesToTranslate) {
                StreamingPropertyValue spv = BcSchema.TEXT.getPropertyValue(property);
                String text = IOUtils.toString(spv.getInputStream(), StandardCharsets.UTF_8);
                if (StringUtils.isEmpty(text)) {
                    continue;
                }

                String sourceLanguage = TextPropertyHelper.getTextLanguage(property);

                try {
                    TranslateTextRequest req = TranslateTextRequest.newBuilder()
                            .setParent(locationName.toString())
                            .setMimeType("text/plain")
                            .setTargetLanguageCode(targetLanguage)
                            .addContents(text)
                            .build();

                    TranslateTextResponse response = googleClient.translateText(req);
                    String translatedText = response.getTranslationsList().get(0).getTranslatedText();

                    PropertyMetadata propertyMetadata = new PropertyMetadata(getUser(), new VisibilityJson(), Visibility.EMPTY);
                    BcSchema.MIME_TYPE_METADATA.setMetadata(propertyMetadata, "text/plain", Visibility.EMPTY);
                    BcSchema.TEXT_DESCRIPTION_METADATA.setMetadata(propertyMetadata, "Translated Text", Visibility.EMPTY);
                    BcSchema.TEXT_LANGUAGE_METADATA.setMetadata(propertyMetadata, targetLanguage, Visibility.EMPTY);

                    Metadata textMetadata = propertyMetadata.createMetadata();
                    String newTextPropertyKey = sourceLanguage + "-" + targetLanguage;
                    BcSchema.TEXT.addPropertyValue(
                            element,
                            newTextPropertyKey,
                            DefaultStreamingPropertyValue.create(translatedText),
                            textMetadata,
                            Visibility.EMPTY,
                            getAuthorizations()
                    );

                    // add also the new language
                    RawObjectSchema.RAW_LANGUAGE.addPropertyValue(element, newTextPropertyKey, targetLanguage,
                            propertyMetadata.createMetadata(), Visibility.EMPTY, getAuthorizations());

                    getGraph().flush();

                    getWorkQueueRepository().pushGraphPropertyQueue(
                            element,
                            newTextPropertyKey,
                            RawObjectSchema.RAW_LANGUAGE.getPropertyName(),
                            data.getWorkspaceId(),
                            data.getVisibilitySource(),
                            Priority.HIGH,
                            ElementOrPropertyStatus.UPDATE,
                            null
                    );

                    getWebQueueRepository().pushTextUpdated(data.getElement().getId(), Priority.HIGH);

                    LOGGER.info("Translated "+text.length()+" characters");
                } catch (Exception ex) {
                    LOGGER.warn("Could not perform translation.", ex);
                }
            }
        } catch (Exception ex) {
            LOGGER.warn("Could not create translation client.", ex);
        }
    }

    public Set<String> getSupportedTranslations() throws IOException {
        try (TranslationServiceClient client = TranslationServiceClient.create()) {
            LocationName parent = LocationName.of(GoogleCredentialUtils.getProjectId(), "global");
            GetSupportedLanguagesRequest request = GetSupportedLanguagesRequest.newBuilder()
                    .setParent(parent.toString())
                    .setDisplayLanguageCode(targetLanguage)
                    .build();
            SupportedLanguages response = client.getSupportedLanguages(request);
            return response.getLanguagesList().stream()
                    .map(SupportedLanguage::getLanguageCode)
                    .collect(Collectors.toSet());
        }
    }
}
