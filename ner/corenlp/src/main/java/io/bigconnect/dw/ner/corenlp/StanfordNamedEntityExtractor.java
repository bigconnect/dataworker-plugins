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
package io.bigconnect.dw.ner.corenlp;

import com.bericotech.clavin.extractor.LocationOccurrence;
import com.mware.core.config.Configuration;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.ge.metric.GeMetricRegistry;
import edu.stanford.nlp.util.Triple;
import io.bigconnect.dw.ner.common.extractor.*;
import io.bigconnect.dw.ner.common.places.substitutions.Blacklist;
import io.bigconnect.dw.ner.common.places.substitutions.CustomSubstitutionMap;
import io.bigconnect.dw.ner.common.places.substitutions.WikipediaDemonymMap;
import edu.stanford.nlp.ie.AbstractSequenceClassifier;
import edu.stanford.nlp.ie.crf.CRFClassifier;
import edu.stanford.nlp.util.CoreMap;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class StanfordNamedEntityExtractor implements EntityExtractor {
    public final static BcLogger LOGGER = BcLoggerFactory.getLogger(StanfordNamedEntityExtractor.class);

    public static final String CONFIGURATION_PREFIX = "entityExtractor";
    public static final String CONFIG_NER_MODEL = CONFIGURATION_PREFIX + ".nerModel";

    public static final String CUSTOM_SUBSTITUTION_FILE = "custom-substitutions.csv";
    public static final String LOCATION_BLACKLIST_FILE = "location-blacklist.txt";
    public static final String PERSON_TO_PLACE_FILE = "person-to-place-replacements.csv";

    // the actual named entity recognizer (NER) object
    private AbstractSequenceClassifier<CoreMap> namedEntityRecognizer;

    private WikipediaDemonymMap demonyms;
    private CustomSubstitutionMap customSubstitutions;
    private CustomSubstitutionMap personToPlaceSubstitutions;
    private Blacklist locationBlacklist;
    private Configuration configuration;
    private GeMetricRegistry metricRegistry;
    private Model model;

    // Don't change the order of this, unless you also change the default in the cliff.properties file
    public enum Model {
        ENGLISH_ALL_3CLASS, ENGLISH_CONLL_4CLASS, SPANISH_ANCORA, GERMAN_DEWAC
    }

    public String getName() {
        return "Stanford CoreNLP NER";
    }

    public void initialize(Configuration configuration, GeMetricRegistry metricRegistry) throws ClassCastException, IOException, ClassNotFoundException {
        this.configuration = configuration;
        this.metricRegistry = metricRegistry;
        String modelToUse = configuration.get(CONFIG_NER_MODEL, "ENGLISH_ALL_3CLASS");
        model = Model.valueOf(modelToUse);
        LOGGER.info("Creating Standford NER with " + modelToUse);
        switch (model) {
            case ENGLISH_ALL_3CLASS:
                initializeWithModelFiles("english.all.3class.caseless.distsim.crf.ser.gz", "english.all.3class.caseless.distsim.prop");
                break;
            case ENGLISH_CONLL_4CLASS:
                initializeWithModelFiles("english.conll.4class.caseless.distsim.crf.ser.gz", "english.conll.4class.caseless.distsim.prop"); // makes it take about 30% longer :-(
                break;
            case SPANISH_ANCORA:
                initializeWithModelFiles("spanish.ancora.distsim.s512.crf.ser.gz", "spanish.ancora.distsim.s512.prop"); // not tested yet
                break;
            case GERMAN_DEWAC:
                initializeWithModelFiles("german.dewac_175m_600.crf.ser.gz", "german.dewac_175m_600.prop"); // not tested yet
                break;
        }
        demonyms = new WikipediaDemonymMap();
        customSubstitutions = new CustomSubstitutionMap(CUSTOM_SUBSTITUTION_FILE);
        locationBlacklist = new Blacklist(LOCATION_BLACKLIST_FILE);
        personToPlaceSubstitutions = new CustomSubstitutionMap(PERSON_TO_PLACE_FILE, false);
    }

    /**
     * Builds a new instance by instantiating the
     * Stanford NER named entity recognizer with a specified
     * language model.
     *
     * @param NERmodel path to Stanford NER language model
     * @param NERprop  path to property file for Stanford NER language model
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws ClassCastException
     */
    //@SuppressWarnings("unchecked")
    private void initializeWithModelFiles(String NERmodel, String NERprop) throws IOException, ClassCastException, ClassNotFoundException {
        InputStream mpis = this.getClass().getClassLoader().getResourceAsStream("models/" + NERprop);
        Properties mp = new Properties();
        mp.load(mpis);
        namedEntityRecognizer = CRFClassifier.getJarClassifier("/models/" + NERmodel, mp);
    }

    /**
     * Get extracted locations from a plain-text body.
     *
     * @param textToParse             Text content to perform extraction on.
     * @param manuallyReplaceDemonyms Can slow down performance quite a bit
     * @return All the entities mentioned
     */
    @Override
    public ExtractedEntities extractEntities(String language, String textToParse, boolean manuallyReplaceDemonyms) {
        ExtractedEntities entities = new ExtractedEntities(configuration, metricRegistry);

        if (textToParse == null || textToParse.length() == 0) {
            LOGGER.warn("input to extractEntities was null or zero!");
            return entities;
        }

        String text = textToParse;
        if (manuallyReplaceDemonyms) {    // this is a noticeable performance hit
            LOGGER.debug("Replacing all demonyms by hand");
            text = demonyms.replaceAll(textToParse);
        }

        // extract entities as <Entity Type, Start Index, Stop Index>
        List<Triple<String, Integer, Integer>> extractedEntities = namedEntityRecognizer.classifyToCharacterOffsets(text);

        if (extractedEntities != null) {
            for (Triple<String, Integer, Integer> extractedEntity : extractedEntities) {
                String entityName = text.substring(extractedEntity.second(), extractedEntity.third());
                int position = extractedEntity.second();
                switch (extractedEntity.first) {
                    case ":PERS":       // spanish
                    case ":I-PER":      // german
                    case "PERSON":      // english
                        if (personToPlaceSubstitutions.contains(entityName)) {
                            entities.addLocation(getLocationOccurrence(personToPlaceSubstitutions.getSubstitution(entityName), position));
                            LOGGER.debug("Changed person " + entityName + " to a place");
                        } else {
                            PersonOccurrence person = new PersonOccurrence(entityName, position);
                            entities.addPerson(person);
                        }
                        break;
                    case ":LUG":        // spanish
                    case ":I-LOC":      // german
                    case "LOCATION":    // english
                        if (!locationBlacklist.contains(entityName)) {
                            entities.addLocation(getLocationOccurrence(entityName, position));
                        } else {
                            LOGGER.debug("Ignored blacklisted location " + entityName);
                        }
                        break;
                    case ":ORG":            // spanish
                    case ":I-ORG":          // german
                    case "ORGANIZATION":    // english
                        OrganizationOccurrence organization = new OrganizationOccurrence(entityName, position);
                        entities.addOrganization(organization);
                        break;
                    case "MISC":    // if you're using the slower 4class model
                        if (demonyms.contains(entityName)) {
                            LOGGER.debug("Found and adding a MISC demonym " + entityName);
                            entities.addLocation(getLocationOccurrence(entityName, position));
                        }
                        break;
                    default:
                        LOGGER.error("Unknown NER type :" + extractedEntity.first);
                }
            }
        }

        return entities;
    }


    /**
     * Get extracted locations from a plain-text body.
     *
     * @param sentences               Text content to perform extraction on.
     * @param manuallyReplaceDemonyms Can slow down performance quite a bit
     * @return All the entities mentioned
     */
    @Override
    @SuppressWarnings("rawtypes")
    public ExtractedEntities extractEntitiesFromSentences(String language, Map[] sentences, boolean manuallyReplaceDemonyms) {
        ExtractedEntities entities = new ExtractedEntities(configuration, metricRegistry);

        if (sentences.length == 0) {
            LOGGER.warn("input to extractEntities was null or zero!");
            return entities;
        }

        if (manuallyReplaceDemonyms) {    // this is a noticeable performance hit
            LOGGER.debug("Replacing all demonyms by hand");
        }

        for (Map s : sentences) {
            String storySentencesId = s.get("story_sentences_id").toString();
            String text = s.get("sentence").toString();
            if (manuallyReplaceDemonyms) {    // this is a noticeable performance hit
                text = demonyms.replaceAll(text);
            }
            // extract entities as <Entity Type, Start Index, Stop Index>
            List<Triple<String, Integer, Integer>> extractedEntities = namedEntityRecognizer.classifyToCharacterOffsets(text);
            if (extractedEntities != null) {
                for (Triple<String, Integer, Integer> extractedEntity : extractedEntities) {
                    String entityName = text.substring(extractedEntity.second(), extractedEntity.third());
                    int position = extractedEntity.second();
                    switch (extractedEntity.first) {
                        case "PERSON":
                            if (personToPlaceSubstitutions.contains(entityName)) {
                                entities.addLocation(getLocationOccurrence(personToPlaceSubstitutions.getSubstitution(entityName), position));
                                LOGGER.debug("Changed person " + entityName + " to a place");
                            } else {
                                PersonOccurrence person = new PersonOccurrence(entityName, position);
                                entities.addPerson(person);
                            }
                            break;
                        case "LOCATION":
                            if (!locationBlacklist.contains(entityName)) {
                                LocationOccurrence loc = getLocationOccurrence(entityName, position);
                                // save the sentence id here
                                entities.addLocation(new SentenceLocationOccurrence(loc.getText(), storySentencesId));
                            } else {
                                LOGGER.debug("Ignored blacklisted location " + entityName);
                            }
                            break;
                        case "ORGANIZATION":
                            OrganizationOccurrence organization = new OrganizationOccurrence(entityName, position);
                            entities.addOrganization(organization);
                            break;
                        case "MISC":    // if you're using the slower 4class model
                            if (demonyms.contains(entityName)) {
                                LOGGER.debug("Found and adding a MISC demonym " + entityName);
                                entities.addLocation(getLocationOccurrence(entityName, position));
                            }
                            break;
                        default:
                            LOGGER.error("Unknown NER type :" + extractedEntity.first);
                    }
                }
            }
        }

        return entities;
    }

    private LocationOccurrence getLocationOccurrence(String entityName, int position) {
        String fixedName = entityName;
        if (demonyms.contains(entityName)) {
            fixedName = demonyms.getSubstitution(entityName);
            LOGGER.debug("Demonym substitution: " + entityName + " to " + fixedName);
        } else if (customSubstitutions.contains(entityName)) {
            fixedName = customSubstitutions.getSubstitution(entityName);
            LOGGER.debug("Custom substitution: " + entityName + " to " + fixedName);
        }
        return new LocationOccurrence(fixedName, position);
    }

}
