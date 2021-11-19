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

import com.google.api.gax.rpc.ResourceExhaustedException;
import com.google.cloud.translate.v3beta1.*;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.mware.core.ingest.dataworker.DataWorker;
import com.mware.core.ingest.dataworker.DataWorkerData;
import com.mware.core.ingest.dataworker.DataWorkerPrepareData;
import com.mware.core.ingest.dataworker.ElementOrPropertyStatus;
import com.mware.core.model.Description;
import com.mware.core.model.Name;
import com.mware.core.model.clientapi.dto.VisibilityJson;
import com.mware.core.model.notification.SystemNotification;
import com.mware.core.model.notification.SystemNotificationRepository;
import com.mware.core.model.notification.SystemNotificationSeverity;
import com.mware.core.model.properties.BcSchema;
import com.mware.core.model.properties.RawObjectSchema;
import com.mware.core.model.properties.types.BcProperty;
import com.mware.core.model.properties.types.PropertyMetadata;
import com.mware.core.model.workQueue.Priority;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.ge.*;
import com.mware.ge.metric.PausableTimerContext;
import com.mware.ge.metric.Timer;
import com.mware.ge.mutation.ExistingElementMutation;
import com.mware.ge.util.Preconditions;
import com.mware.ge.values.storable.DefaultStreamingPropertyValue;
import com.mware.ge.values.storable.StreamingPropertyValue;
import com.mware.ge.values.storable.TextValue;
import com.mware.ge.values.storable.Values;
import io.bigconnect.dw.google.common.schema.GoogleCredentialUtils;
import io.bigconnect.dw.text.common.LanguageDetectorUtil;
import io.bigconnect.dw.text.common.TextPropertyHelper;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
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
    private SystemNotificationRepository systemNotificationRepository;
    private Timer detectTimer;

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
        languageDetector = new LanguageDetectorUtil();
        detectTimer = getGraph().getMetricsRegistry().getTimer(getClass(), "translate-time");
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

    /**
     * Translation is made for TITLE and TEXT using the following algorithm:
     *  1. find all translatable properties (properties that have BcSchema.TEXT_LANGUAGE_METADATA.getMetadataKey() in the list of supportedLanguages)
     *  2. perform the translation
     *  3. if translation is successful, add the translated property with the same name and the following additional things:
     *     - BcSchema.TEXT_LANGUAGE_METADATA will have the target language
     *     - property key: sourceLanguage + "-" + targetLanguage;
     *  4. mark translation as successful if both properties were translated or failed if any of them failed
     */
    @Override
    public void execute(InputStream in, DataWorkerData data) throws Exception {
        Element element = refresh(data.getElement());

        boolean success = true;
        success = success && translateTextProperties(element, data);
        success = success && translateTitleProperties(element, data);

        if (success) {
            // translation successful, don't translate again
            GOOGLE_TRANSLATE_PROPERTY.setProperty(element, Boolean.FALSE, Visibility.EMPTY, element.getAuthorizations());
        }
        else {
            // translation not successful, translate again later
            GOOGLE_TRANSLATE_PROPERTY.setProperty(element, Boolean.TRUE, Visibility.EMPTY, element.getAuthorizations());
        }

        getGraph().flush();
    }

    private boolean translateTitleProperties(Element element, DataWorkerData data) {
        List<Property> propertiesToTranslate = findKeyedPropertiesToTranslate(element, BcSchema.TITLE);
        if (propertiesToTranslate.isEmpty()) {
            // success
            return true;
        }

        try (TranslationServiceClient googleClient = TranslationServiceClient.create()) {
            for (Property property : propertiesToTranslate) {
                String title = BcSchema.TITLE.getPropertyValue(property);
                if (StringUtils.isEmpty(title)) {
                    continue;
                }

                try {
                    String sourceLanguage = TextPropertyHelper.getTextLanguage(property);
                    TranslateTextRequest req = TranslateTextRequest.newBuilder()
                            .setParent(locationName.toString())
                            .setMimeType("text/plain")
                            .setTargetLanguageCode(targetLanguage)
                            .addContents(title)
                            .build();
                    PausableTimerContext t = new PausableTimerContext(detectTimer);
                    TranslateTextResponse response = googleClient.translateText(req);
                    t.stop();
                    String translatedText = response.getTranslationsList().get(0).getTranslatedText();
                    PropertyMetadata propertyMetadata = new PropertyMetadata(getUser(), new VisibilityJson(), Visibility.EMPTY);
                    BcSchema.TEXT_LANGUAGE_METADATA.setMetadata(propertyMetadata, targetLanguage, Visibility.EMPTY);
                    Metadata textMetadata = propertyMetadata.createMetadata();
                    String newTextPropertyKey = sourceLanguage + "-" + targetLanguage;
                    BcSchema.TITLE.addPropertyValue(
                            element,
                            newTextPropertyKey,
                            translatedText,
                            textMetadata,
                            Visibility.EMPTY,
                            getAuthorizations()
                    );

                    logElement(element, sourceLanguage, title);

                    // success
                    return true;
                } catch (Exception ex) {
                    LOGGER.warn("Could not perform translation: " + ex.getMessage());
                    // failure
                    return false;
                }
            }
        } catch (Exception ex) {
            LOGGER.warn("Could not create translation client.", ex);
            // failure
            return false;
        }

        // success
        return true;
    }

    private boolean translateTextProperties(Element element, DataWorkerData data) {
        List<Property> propertiesToTranslate = findKeyedPropertiesToTranslate(element, BcSchema.TEXT);
        if (propertiesToTranslate.isEmpty()) {
            return true;
        }

        try (TranslationServiceClient googleClient = TranslationServiceClient.create()) {
            for (Property property : propertiesToTranslate) {
                StreamingPropertyValue spv = BcSchema.TEXT.getPropertyValue(property);
                String text = IOUtils.toString(spv.getInputStream(), StandardCharsets.UTF_8);
                if (StringUtils.isEmpty(text)) {
                    continue;
                }

                try {
                    String sourceLanguage = TextPropertyHelper.getTextLanguage(property);
                    TranslateTextRequest req = TranslateTextRequest.newBuilder()
                            .setParent(locationName.toString())
                            .setMimeType("text/plain")
                            .setTargetLanguageCode(targetLanguage)
                            .addContents(text)
                            .build();

                    PausableTimerContext t = new PausableTimerContext(detectTimer);
                    TranslateTextResponse response = googleClient.translateText(req);
                    t.stop();

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

                    getWorkQueueRepository().pushOnDwQueue(
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
                    logElement(element, sourceLanguage, text);

                    // success
                    return true;
                } catch (Exception ex) {
                    if (ex instanceof ResourceExhaustedException) {
                        sendQuotaExceededNotification();
                    }

                    LOGGER.warn("Could not perform translation: " + ex.getMessage());
                    // failure
                    return false;
                }
            }
        } catch (Exception ex) {
            LOGGER.warn("Could not create translation client.", ex);
            // failure
            return false;
        }

        // success
        return true;
    }

    private void sendQuotaExceededNotification() {
        long count = systemNotificationRepository.getActiveNotifications(null)
                .stream().filter(n -> "translateQuota".equals(n.getActionEvent()))
                .count();

        if (count == 0) {
            Date startDate = new Date();
            Date endDate = new Date();
            DateUtils.setHours(endDate, 23);
            DateUtils.setMinutes(endDate, 59);
            String ddMM = new SimpleDateFormat("dd/MM").format(startDate);
            SystemNotification notif = systemNotificationRepository.createNotification(
                    SystemNotificationSeverity.WARNING,
                    "Google Translate Daily Limit",
                    ddMM + ": The daily limit for Google Translate has been reached.",
                    "translateQuota", new JSONObject(), startDate, endDate, null
            );

            getWebQueueRepository().pushSystemNotification(notif);
        }
    }

    private List<Property> findKeyedPropertiesToTranslate(Element element, BcProperty<?> propertyToTranslate) {
        List<Property> propertiesToTranslate = new ArrayList<>();

        for (Property property : propertyToTranslate.getProperties(element)) {
            String language = TextPropertyHelper.getTextLanguage(property);
            if (StringUtils.isEmpty(language)) {
                String strPropValue = null;
                if (property.getValue() instanceof StreamingPropertyValue)
                    strPropValue = ((StreamingPropertyValue) property.getValue()).readToString();
                else if (property.getValue() instanceof TextValue)
                    strPropValue = ((TextValue) property.getValue()).stringValue();
                else
                    strPropValue = property.getValue().prettyPrint();

                language = languageDetector
                        .detectLanguage(strPropValue)
                        .orElse("");

                if (!StringUtils.isEmpty(language)) {
                    ExistingElementMutation<Vertex> m = element.prepareMutation();
                    m.setPropertyMetadata(property, BcSchema.TEXT_LANGUAGE_METADATA.getMetadataKey(),
                            Values.stringValue(language), Visibility.EMPTY);
                    element = m.save(getAuthorizations());
                }
            }
            if (StringUtils.isEmpty(language) || !targetLanguage.equals(language)) {
                boolean canTranslate = StringUtils.isEmpty(language) || supportedLanguages.contains(language);
                if (canTranslate)
                    propertiesToTranslate.add(property);
            }
        }

        return propertiesToTranslate;
    }

    private void logElement(Element element, String sourceLanguage, String text) {
        final String SEPARATOR = "|$";
        Vertex v = (Vertex) element;

        StringBuilder sb = new StringBuilder();
        sb
                .append('\n')
                .append("gTranslateLog_8365775793").append(SEPARATOR)
                .append(text.length()).append(SEPARATOR)
                .append(v.getPropertyValue("createdDate")).append(SEPARATOR)
                .append(v.getId()).append(SEPARATOR)
                .append(v.getPropertyValue("title")).append(SEPARATOR)
                .append(v.getConceptType()).append(SEPARATOR)
                .append(v.getPropertyValue("source")).append(SEPARATOR)
                .append(sourceLanguage).append(SEPARATOR)
                .append(v.getTimestamp());

        LOGGER.warn(sb.toString());
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

    @Inject
    public void setSystemNotificationRepository(SystemNotificationRepository systemNotificationRepository) {
        this.systemNotificationRepository = systemNotificationRepository;
    }
}
