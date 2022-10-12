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
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.ge.metric.GeMetricRegistry;
import com.mware.ge.metric.PausableTimerContext;
import com.mware.ge.metric.Timer;
import com.mware.ge.util.Preconditions;
import io.bigconnect.dw.ner.common.extractor.*;
import io.bigconnect.dw.ner.common.places.substitutions.WikipediaDemonymMap;
import okhttp3.OkHttpClient;
import org.apache.commons.lang3.StringUtils;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.bigconnect.dw.ner.intellidockers.IntelliDockersSchemaContribution.*;

public class IntelliDockersNamedEntityExtractorWithSentiment implements EntityExtractor {
    public final static BcLogger LOGGER = BcLoggerFactory.getLogger(IntelliDockersNamedEntityExtractorWithSentiment.class);
    public static final String CONFIG_INTELLIDOCKERS_URL = "intellidockers.ron.ner.url";
    public static final String CONFIG_INTELLIDOCKERS_SCORE = "intellidockers.ron.ner.score";

    private Configuration configuration;
    private WikipediaDemonymMap demonyms;
    private IntelliDockersNer service;
    private GeMetricRegistry metricRegistry;
    private Timer detectTimer;
    private double score;

    @Override
    public void initialize(Configuration config, GeMetricRegistry metricRegistry) throws ClassCastException {
        this.configuration = config;
        this.metricRegistry = metricRegistry;
        demonyms = new WikipediaDemonymMap();
        String url = config.get(CONFIG_INTELLIDOCKERS_URL, null);
        String strScore = config.get(CONFIG_INTELLIDOCKERS_SCORE, "0.7");
        score = Double.parseDouble(strScore);

        Preconditions.checkState(!StringUtils.isEmpty(url), "Please provide the '" + CONFIG_INTELLIDOCKERS_URL + "' config parameter");

        OkHttpClient client = new OkHttpClient().newBuilder()
                .callTimeout(1, TimeUnit.MINUTES)
                .readTimeout(1, TimeUnit.MINUTES)
                .connectTimeout(3, TimeUnit.SECONDS)
                .build();

        Retrofit retrofit = new Retrofit.Builder()
                .baseUrl(url)
                .client(client)
                .addConverterFactory(JacksonConverterFactory.create())
                .build();

        service = retrofit.create(IntelliDockersNer.class);
        detectTimer = metricRegistry.getTimer(getClass(), "ner-time");
    }

    @Override
    public ExtractedEntities extractEntities(String language, String textToParse, boolean manuallyReplaceDemonyms) {
        ExtractedEntities entities = new ExtractedEntities(configuration, metricRegistry);
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
            PausableTimerContext t = new PausableTimerContext(detectTimer);
            Response<Map<String, Entity>> response = service.process(new NerRequest(text, "ron"))
                    .execute();
            t.stop();

            if (response.isSuccessful() && response.body() != null) {
                Map<String, Entity> body = response.body();
                if (body != null && body.size() > 0) {
                    for (Entity entity : body.values()) {
                        if (entity.details.size() == 0)
                            continue;

                        Entity.EntityDetail detail = entity.details.get(0).get(0);
                        if (detail.score < score)
                            continue;

                        int start = detail.start;
                        int sent = 0;
                        if ("neg".equals(entity.sentiment.label))
                            sent = -1;
                        else if ("pos".equals(entity.sentiment.label))
                            sent = 1;

                        double sentScore = entity.sentiment.score;

                        switch (entity.type) {
                            case "PERSON":
                                entities.addPerson(new PersonOccurrence(entity.entity, start, sent, sentScore));
                                break;
                            case "ORGANIZATION":
                                entities.addOrganization(new OrganizationOccurrence(entity.entity, start, sent, score));
                                break;
                            case "LOCATION":
                                entities.addLocation(new ExtLocationOccurence(entity.entity, start, sent, score));
                                break;
                            case "NATIONALITY":
                                entities.addGenericEntity(new GenericOccurrence(entity.entity, CONCEPT_TYPE_NATIONALITY, start, sent, score));
                                break;
                            case "RELIGION":
                                entities.addGenericEntity(new GenericOccurrence(entity.entity, CONCEPT_TYPE_RELIGION, start, sent, score));
                                break;
                            case "IDENTIFIER_CREDIT_CARD_NUM":
                                entities.addGenericEntity(new GenericOccurrence(entity.entity, CONCEPT_TYPE_CREDIT_CARD, start, sent, score));
                                break;
                            case "IDENTIFIER_EMAIL":
                                entities.addGenericEntity(new GenericOccurrence(entity.entity, CONCEPT_TYPE_EMAIL, start, sent, score));
                                break;
                            case "IDENTIFIER_PERSONAL_ID_NUM":
                                entities.addGenericEntity(new GenericOccurrence(entity.entity, CONCEPT_TYPE_PERSONAL_ID, start, sent, score));
                                break;
                            case "IDENTIFIER_PHONE_NUMBER":
                                entities.addGenericEntity(new GenericOccurrence(entity.entity, CONCEPT_TYPE_PHONE_NUMBER, start, sent, score));
                                break;
                            case "IDENTIFIER_URL":
                                entities.addGenericEntity(new GenericOccurrence(entity.entity, CONCEPT_TYPE_URL, start, sent, score));
                                break;
                        }
                    }
                } else {
                    LOGGER.error("Could not extract entities. Code: %s, body: %s", response.errorBody(), response.code());
                }
            } else{
                LOGGER.error("Got unsuccessfully error.\n code: %s, body: %s", response.code(), response.errorBody());
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
