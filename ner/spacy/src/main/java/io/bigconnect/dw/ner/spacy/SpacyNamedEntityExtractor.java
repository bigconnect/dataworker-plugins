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
package io.bigconnect.dw.ner.spacy;

import com.bericotech.clavin.extractor.LocationOccurrence;
import com.mware.core.config.Configuration;
import com.mware.core.process.ExecUtils;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.ge.metric.GeMetricRegistry;
import io.bigconnect.dw.ner.common.extractor.*;
import io.bigconnect.dw.ner.common.places.substitutions.Blacklist;
import io.bigconnect.dw.ner.common.places.substitutions.CustomSubstitutionMap;
import io.bigconnect.dw.ner.common.places.substitutions.WikipediaDemonymMap;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.mware.core.config.FileConfigurationLoader.ENV_BC_DIR;

public class SpacyNamedEntityExtractor implements EntityExtractor {
    public final static BcLogger LOGGER = BcLoggerFactory.getLogger(SpacyNamedEntityExtractor.class);

    public static final String CUSTOM_SUBSTITUTION_FILE = "custom-substitutions.csv";
    public static final String LOCATION_BLACKLIST_FILE = "location-blacklist.txt";
    public static final String PERSON_TO_PLACE_FILE = "person-to-place-replacements.csv";

    private Configuration configuration;
    private GeMetricRegistry metricRegistry;
    private WikipediaDemonymMap demonyms;
    private CustomSubstitutionMap customSubstitutions;
    private CustomSubstitutionMap personToPlaceSubstitutions;
    private Blacklist locationBlacklist;

    @Override
    public void initialize(Configuration configuration, GeMetricRegistry metricRegistry) throws ClassCastException, IOException, ClassNotFoundException {
        this.configuration = configuration;
        this.metricRegistry = metricRegistry;
        demonyms = new WikipediaDemonymMap();
        customSubstitutions = new CustomSubstitutionMap(CUSTOM_SUBSTITUTION_FILE);
        locationBlacklist = new Blacklist(LOCATION_BLACKLIST_FILE);
        personToPlaceSubstitutions = new CustomSubstitutionMap(PERSON_TO_PLACE_FILE, false);
    }

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

        ProcessBuilder pb = new ProcessBuilder();
        pb.directory(getPythonLibPath().toFile());
        pb.command(getCommand(language, text));

        try {
            ExecUtils.ExecutionResults results = ExecUtils.execAndGetOutputAndErrors(pb);
            if(results.rv == 0) {
                String[] ents = StringUtils.split(results.out, '\n');
                for(String ent : ents) {
                    String[] d = StringUtils.split(ent, (char) 0x1f);
                    if (d.length != 5)
                        continue;

                    String entityName = d[0];
                    String type = d[1];
                    int start = Integer.parseInt(d[2])-1;
                    int end = Integer.parseInt(d[3])-1;
                    float sentiment = Float.parseFloat(d[4]);

                    switch (type) {
                        case "PERSON":
                        case "PER":
                            if (personToPlaceSubstitutions.contains(entityName)) {
                                entities.addLocation(getLocationOccurrence(personToPlaceSubstitutions.getSubstitution(entityName), start));
                                LOGGER.debug("Changed person " + entityName + " to a place");
                            } else {
                                PersonOccurrence person = new PersonOccurrence(entityName, start);
                                entities.addPerson(person);
                            }
                            break;
                        case "ORG":
                            OrganizationOccurrence organization = new OrganizationOccurrence(entityName, start);
                            entities.addOrganization(organization);
                            break;
                        case "GPE":
                        case "LOC":
                            if (!locationBlacklist.contains(entityName)) {
                                entities.addLocation(getLocationOccurrence(entityName, start));
                            } else {
                                LOGGER.debug("Ignored blacklisted location " + entityName);
                            }
                            break;
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return entities;
    }

    @Override
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

            ProcessBuilder pb = new ProcessBuilder();
            pb.directory(getPythonLibPath().toFile());
            pb.command(getCommand(language, text));

            try {
                ExecUtils.ExecutionResults results = ExecUtils.execAndGetOutputAndErrors(pb);
                if(results.rv == 0) {
                    String[] ents = StringUtils.split(results.out, '\n');
                    for(String ent : ents) {
                        String[] d = StringUtils.split(ent, (char) 0x1f);
                        if (d.length != 5)
                            continue;

                        String entityName = d[0];
                        String type = d[1];
                        int start = Integer.parseInt(d[2])-1;
                        int end = Integer.parseInt(d[3])-1;
                        float sentiment = Float.parseFloat(d[4]);

                        switch (type) {
                            case "PERSON":
                            case "PER":
                                if (personToPlaceSubstitutions.contains(entityName)) {
                                    entities.addLocation(getLocationOccurrence(personToPlaceSubstitutions.getSubstitution(entityName), start));
                                    LOGGER.debug("Changed person " + entityName + " to a place");
                                } else {
                                    PersonOccurrence person = new PersonOccurrence(entityName, start);
                                    entities.addPerson(person);
                                }
                                break;
                            case "ORG":
                                OrganizationOccurrence organization = new OrganizationOccurrence(entityName, start);
                                entities.addOrganization(organization);
                                break;
                            case "GPE":
                            case "LOC":
                                if (!locationBlacklist.contains(entityName)) {
                                    LocationOccurrence loc = getLocationOccurrence(entityName, start);
                                    // save the sentence id here
                                    entities.addLocation(new SentenceLocationOccurrence(loc.getText(), storySentencesId));
                                } else {
                                    LOGGER.debug("Ignored blacklisted location " + entityName);
                                }
                                break;
                        }
                    }
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        return entities;
    }

    @Override
    public String getName() {
        return "Spacy NER";
    }

    public static Path getPythonLibPath() {
        return Paths.get(System.getenv(ENV_BC_DIR)).resolve("lib").resolve("python");
    }

    private List<String> getCommand(String language, String text) {
        return Arrays.asList(
                getPythonBinary().toFile().getAbsolutePath(),
                getPythonLibPath().resolve("nlp").resolve("ner.py").toFile().getAbsolutePath(),
                language,
                "\""+text+"\""
        );
    }

    public static Path getPythonBinary() {
        return new File(System.getenv("BCPYTHONBIN")).toPath();
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
