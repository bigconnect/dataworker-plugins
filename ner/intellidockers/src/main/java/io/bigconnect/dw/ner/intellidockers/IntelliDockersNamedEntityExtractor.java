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
package io.bigconnect.dw.ner.intellidockers;

import com.bericotech.clavin.extractor.LocationOccurrence;
import com.mware.core.config.Configuration;
import com.mware.core.config.HashMapConfigurationLoader;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.ge.util.Preconditions;
import io.bigconnect.dw.ner.common.extractor.*;
import io.bigconnect.dw.ner.common.places.substitutions.WikipediaDemonymMap;
import org.apache.commons.lang3.StringUtils;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static io.bigconnect.dw.ner.intellidockers.IntelliDockersSchemaContribution.*;

public class IntelliDockersNamedEntityExtractor implements EntityExtractor {
    public final static BcLogger LOGGER = BcLoggerFactory.getLogger(IntelliDockersNamedEntityExtractor.class);
    public static final String CONFIG_INTELLIDOCKERS_URL = "intellidockers.ron.ner.url";

    private Configuration configuration;
    private WikipediaDemonymMap demonyms;
    private IntelliDockersNer service;

    @Override
    public void initialize(Configuration config) throws ClassCastException {
        this.configuration = config;
        demonyms = new WikipediaDemonymMap();
        String url = config.get(CONFIG_INTELLIDOCKERS_URL, null);
        Preconditions.checkState(!StringUtils.isEmpty(url), "Please provide the '" + CONFIG_INTELLIDOCKERS_URL + "' config parameter");

        Retrofit retrofit = new Retrofit.Builder()
                .baseUrl(url)
                .addConverterFactory(JacksonConverterFactory.create())
                .build();

        service = retrofit.create(IntelliDockersNer.class);
    }

    @Override
    public ExtractedEntities extractEntities(String language, String textToParse, boolean manuallyReplaceDemonyms) {
        ExtractedEntities entities = new ExtractedEntities(configuration);
        if (textToParse == null || textToParse.length() == 0) {
            LOGGER.warn("input to extractEntities was null or zero!");
            return entities;
        }

        if (!StringUtils.equalsAnyIgnoreCase(language, "ro")) {
            LOGGER.debug("Language %s not supported by %s", language, getClass().getSimpleName());
            return entities;
        }

        String text = textToParse;
        if (manuallyReplaceDemonyms) {    // this is a noticeable performance hit
            LOGGER.debug("Replacing all demonyms by hand");
            text = demonyms.replaceAll(textToParse);
        }

        try {
            Response<Entities> response = service.process(new NerRequest(text, "ron"))
                    .execute();

            if (response.isSuccessful() && response.body() != null) {
                for (Entities.Entity entity : response.body().entities) {
                    int start = StringUtils.indexOf(text, entity.entity);
                    if (start < 0) {
                        LOGGER.debug("Could not find detected entity in text: "+entity.entity);
                        continue;
                    }

                    switch (entity.type) {
                        case "PERSON":
                            entities.addPerson(new PersonOccurrence(entity.entity, start));
                            break;
                        case "ORGANIZATION":
                            entities.addOrganization(new OrganizationOccurrence(entity.entity, start));
                            break;
                        case "LOCATION":
                            entities.addLocation(new LocationOccurrence(entity.entity, start));
                            break;
                        case "NATIONALITY":
                            entities.addGenericEntity(new GenericOccurrence(entity.entity, CONCEPT_TYPE_NATIONALITY, start));
                            break;
                        case "RELIGION":
                            entities.addGenericEntity(new GenericOccurrence(entity.entity, CONCEPT_TYPE_RELIGION, start));
                            break;
                        case "IDENTIFIER_CREDIT_CARD_NUM":
                            entities.addGenericEntity(new GenericOccurrence(entity.entity, CONCEPT_TYPE_CREDIT_CARD, start));
                            break;
                        case "IDENTIFIER_EMAIL":
                            entities.addGenericEntity(new GenericOccurrence(entity.entity, CONCEPT_TYPE_EMAIL, start));
                            break;
                        case "IDENTIFIER_PERSONAL_ID_NUM":
                            entities.addGenericEntity(new GenericOccurrence(entity.entity, CONCEPT_TYPE_PERSONAL_ID, start));
                            break;
                        case "IDENTIFIER_PHONE_NUMBER":
                            entities.addGenericEntity(new GenericOccurrence(entity.entity, CONCEPT_TYPE_PHONE_NUMBER, start));
                            break;
                        case "IDENTIFIER_URL":
                            entities.addGenericEntity(new GenericOccurrence(entity.entity, CONCEPT_TYPE_URL, start));
                            break;
                    }
                }
            }
        } catch (IOException e) {
            LOGGER.warn("Could not extract entities: %s", e.getMessage());
        }

        return entities;
    }

    @Override
    public ExtractedEntities extractEntitiesFromSentences(String language, Map[] sentences, boolean manuallyReplaceDemonyms) {
        return null;
    }

    @Override
    public String getName() {
        return "IntelliDockers NER";
    }
}
