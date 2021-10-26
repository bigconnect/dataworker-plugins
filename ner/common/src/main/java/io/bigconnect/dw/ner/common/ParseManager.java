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
package io.bigconnect.dw.ner.common;

import com.bericotech.clavin.gazetteer.CountryCode;
import com.bericotech.clavin.gazetteer.GeoName;
import com.bericotech.clavin.gazetteer.query.Gazetteer;
import com.bericotech.clavin.gazetteer.query.LuceneGazetteer;
import com.bericotech.clavin.resolver.ResolvedLocation;
import com.google.gson.Gson;
import com.mware.core.config.Configuration;
import com.mware.ge.metric.GeMetricRegistry;
import io.bigconnect.dw.ner.common.extractor.EntityExtractorService;
import io.bigconnect.dw.ner.common.extractor.ExtractedEntities;
import io.bigconnect.dw.ner.common.extractor.SentenceLocationOccurrence;
import io.bigconnect.dw.ner.common.people.ResolvedPerson;
import io.bigconnect.dw.ner.common.places.Adm1GeoNameLookup;
import io.bigconnect.dw.ner.common.places.CliffLocationResolver;
import io.bigconnect.dw.ner.common.places.CountryGeoNameLookup;
import io.bigconnect.dw.ner.common.places.UnknownGeoNameIdException;
import io.bigconnect.dw.ner.common.places.focus.FocusLocation;
import io.bigconnect.dw.ner.common.places.focus.FocusStrategy;
import io.bigconnect.dw.ner.common.places.focus.FrequencyOfMentionFocusStrategy;
import io.bigconnect.dw.ner.common.orgs.ResolvedOrganization;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Singleton-style wrapper around a GeoParser.  Call GeoParser.locate(someText) to use this class.
 */
public class ParseManager {
    public static final String CONFIGURATION_PREFIX = "entityExtractor";
    public static final String GEOINDEX_PATH = CONFIGURATION_PREFIX + ".geoIndexPath";

    /**
     * Major: major new features or capabilities
     * Minor: small new features, changes to the json result format, or changes to the disambiguation algorithm
     * Revision: minor change or bug fix
     */
    static final String PARSER_VERSION = "2.4.2";

    private static final Logger logger = LoggerFactory.getLogger(ParseManager.class);

    public static EntityParser parser = null;

    private static CliffLocationResolver resolver;   // HACK: pointer to keep around for stats logging

    private static FocusStrategy focusStrategy;

    // these two are the statuses used in the JSON responses
    public static final String STATUS_OK = "ok";
    public static final String STATUS_ERROR = "error";

    @SuppressWarnings({"rawtypes", "unchecked"})
    public static HashMap getResponseMap(HashMap results) {
        HashMap response = new HashMap();
        response.put("status", STATUS_OK);
        response.put("version", PARSER_VERSION);
        response.put("results", results);
        return response;
    }

    public static GeoName getGeoName(int id) throws UnknownGeoNameIdException {
        GeoName geoname = resolver.getByGeoNameId(id);
        return geoname;
    }

    @SuppressWarnings({"rawtypes"})
    public static HashMap getGeoNameInfo(int id, Configuration config, GeMetricRegistry metricRegistry) {
        return getGeoNameInfo(id, true, config, metricRegistry);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public static HashMap getGeoNameInfo(int id, boolean withAncestry, Configuration config, GeMetricRegistry metricRegistry) {
        try {
            GeoName geoname = getGeoName(id);
            HashMap info = writeGeoNameToHash(geoname, config, metricRegistry);
            if (withAncestry) {
                HashMap childInfo = info;
                GeoName child = geoname;
                while (child.getParent() != null) {
                    GeoName parent = child.getParent();
                    HashMap parentInfo = writeGeoNameToHash(parent, config, metricRegistry);
                    childInfo.put("parent", parentInfo);
                    child = parent;
                    childInfo = parentInfo;
                }
            }
            HashMap response = getResponseMap(info);
            return response;
        } catch (UnknownGeoNameIdException e) {
            logger.warn(e.getMessage());
            return getErrorText("Invalid GeoNames id " + id);
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public static HashMap writeGeoNameToHash(GeoName place, Configuration configuration, GeMetricRegistry metricRegistry) {
        HashMap loc = new HashMap();
        loc.put("id", place.getGeonameID());
        loc.put("name", place.getName());
        loc.put("lat", place.getLatitude());
        loc.put("lon", place.getLongitude());
        loc.put("population", place.getPopulation());
        String featureCode = place.getFeatureCode().toString();
        loc.put("featureClass", place.getFeatureClass().toString());
        loc.put("featureCode", featureCode);
        // add in country info
        String primaryCountryCodeAlpha2 = "";
        if (place.getPrimaryCountryCode() != CountryCode.NULL) {
            primaryCountryCodeAlpha2 = place.getPrimaryCountryCode().toString();
        }
        loc.put("countryCode", primaryCountryCodeAlpha2);
        GeoName countryGeoName = CountryGeoNameLookup.lookup(primaryCountryCodeAlpha2, configuration, metricRegistry);
        String countryGeoNameId = "";
        if (countryGeoName != null) {
            countryGeoNameId = "" + countryGeoName.getGeonameID();
        }
        loc.put("countryGeoNameId", countryGeoNameId);
        // add in state info
        String admin1Code = "";
        if (place.getAdmin1Code() != null) {
            admin1Code = place.getAdmin1Code();
        }
        loc.put("stateCode", admin1Code);
        GeoName adm1GeoName = Adm1GeoNameLookup.lookup(primaryCountryCodeAlpha2, admin1Code, configuration, metricRegistry);
        String stateGeoNameId = "";
        if (adm1GeoName != null) {
            stateGeoNameId = "" + adm1GeoName.getGeonameID();
        }
        loc.put("stateGeoNameId", stateGeoNameId);

        return loc;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public static HashMap writeAboutnessLocationToHash(FocusLocation location, Configuration configuration, GeMetricRegistry metricRegistry) {
        HashMap loc = writeGeoNameToHash(location.getGeoName(), configuration, metricRegistry);
        loc.put("score", location.getScore());
        return loc;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public static HashMap writeResolvedLocationToHash(ResolvedLocation resolvedLocation, Configuration configuration, GeMetricRegistry metricRegistry) {
        HashMap loc = writeGeoNameToHash(resolvedLocation.getGeoname(), configuration, metricRegistry);
        int charIndex = resolvedLocation.getLocation().getPosition();
        loc.put("confidence", resolvedLocation.getConfidence()); // low is good
        HashMap sourceInfo = new HashMap();
        sourceInfo.put("string", resolvedLocation.getLocation().getText());
        sourceInfo.put("charIndex", charIndex);
        if (resolvedLocation.getLocation() instanceof SentenceLocationOccurrence) {
            sourceInfo.put("storySentencesId", ((SentenceLocationOccurrence) resolvedLocation.getLocation()).storySentenceId);
        }
        loc.put("source", sourceInfo);
        return loc;
    }

    public static ExtractedEntities extractAndResolve(Configuration config, GeMetricRegistry metricRegistry, String languageCode, String text) throws Exception {
        return extractAndResolve(config, metricRegistry, languageCode, text, false);
    }

    public static ExtractedEntities extractAndResolve(Configuration config, GeMetricRegistry metricRegistry, String languageCode, String text, boolean manuallyReplaceDemonyms) throws Exception {
        return getParserInstance(config, metricRegistry).extractAndResolve(languageCode, text, manuallyReplaceDemonyms);
    }

    @SuppressWarnings("rawtypes")
    public static ExtractedEntities extractAndResolveFromSentences(Configuration config, GeMetricRegistry metricRegistry, String languageCode, Map[] sentences, boolean manuallyReplaceDemonyms) throws Exception {
        return getParserInstance(config, metricRegistry).extractAndResolveFromSentences(languageCode, sentences, manuallyReplaceDemonyms);
    }

    /**
     * We want all error messages sent to the client to have the same format
     *
     * @param msg
     * @return
     */
    @SuppressWarnings({"unchecked", "rawtypes"})  // I'm generating JSON... don't whine!
    public static HashMap getErrorText(String msg) {
        HashMap info = new HashMap();
        info.put("version", PARSER_VERSION);
        info.put("status", STATUS_ERROR);
        info.put("details", msg);
        return info;
    }

    public static void logStats() {
        if (resolver != null) {
            resolver.logStats();
        }
    }

    /**
     * Lazy instantiation of singleton parser
     */
    public static EntityParser getParserInstance(Configuration config, GeMetricRegistry metricRegistry) throws Exception {
        if (parser == null) {
            focusStrategy = new FrequencyOfMentionFocusStrategy(config, metricRegistry);
            // use the Stanford NER location stanford
            EntityExtractorService extractor = EntityExtractorService.getInstance(config, metricRegistry);
            extractor.initialize(config, metricRegistry);

            boolean useFuzzyMatching = false;
            Gazetteer gazetteer = null;
            File gazetteerDir = new File(config.get(GEOINDEX_PATH, ""));
            if (!gazetteerDir.exists() || !gazetteerDir.isDirectory()) {
                logger.error("Missing gazetter! Download and build a CLAVIN IndexDirectory at " + config.get(GEOINDEX_PATH, ""));
            } else {
                logger.info("Loading CLAVIN Gazetteer from " + config.get(GEOINDEX_PATH, ""));
                gazetteer = new LuceneGazetteer(new File(config.get(GEOINDEX_PATH, "")));
            }

            resolver = new CliffLocationResolver(gazetteer);

            parser = new EntityParser(extractor, metricRegistry, resolver,
                    useFuzzyMatching, CliffLocationResolver.MAX_HIT_DEPTH);

            logger.info("Created parser successfully");
        }

        return parser;
    }

    public static CliffLocationResolver getLocationResolver(Configuration configuration, GeMetricRegistry metricRegistry) throws Exception {
        ParseManager.getParserInstance(configuration, metricRegistry);
        return resolver;
    }

    public static FocusStrategy getFocusStrategy(Configuration configuration, GeMetricRegistry metricRegistry) throws Exception {
        ParseManager.getParserInstance(configuration, metricRegistry);
        return focusStrategy;
    }

    public static CliffLocationResolver getResolver(Configuration configuration, GeMetricRegistry metricRegistry) throws Exception {
        ParseManager.getParserInstance(configuration, metricRegistry);
        return resolver;
    }
}
