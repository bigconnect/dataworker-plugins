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
