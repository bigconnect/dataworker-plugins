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
package io.bigconnect.dw.ner.common.places.focus;

import com.bericotech.clavin.gazetteer.CountryCode;
import com.bericotech.clavin.gazetteer.GeoName;
import com.bericotech.clavin.resolver.ResolvedLocation;
import com.mware.core.config.Configuration;
import com.mware.ge.metric.GeMetricRegistry;
import io.bigconnect.dw.ner.common.places.Adm1GeoNameLookup;
import io.bigconnect.dw.ner.common.places.CountryGeoNameLookup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Once we have selected the candidates, we need to pick what country the document is "about".  This
 * is the most naive "Aboutness" strategy; it just picks the most mentioned country.
 *
 * @author rahulb
 */
public class FrequencyOfMentionFocusStrategy implements FocusStrategy {

    private static final Logger logger = LoggerFactory.getLogger(FrequencyOfMentionFocusStrategy.class);
    private Configuration configuration;
    private GeMetricRegistry metricRegistry;

    public FrequencyOfMentionFocusStrategy(Configuration configuration, GeMetricRegistry metricRegistry) {
        this.configuration = configuration;
        this.metricRegistry = metricRegistry;
    }

    @Override
    public List<FocusLocation> selectCountries(List<ResolvedLocation> resolvedLocations) {
        List<FocusLocation> results = new ArrayList<FocusLocation>();
        // count country mentions
        HashMap<CountryCode, Integer> countryCounts = FocusUtils.getCountryCounts(resolvedLocations);
        if (countryCounts.size() == 0) {
            return results;
        }
        // find the most mentioned
        CountryCode primaryCountry = null;
        for (CountryCode countryCode : countryCounts.keySet()) {
            if ((primaryCountry == null) || (countryCounts.get(countryCode) > countryCounts.get(primaryCountry))) {
                primaryCountry = countryCode;
            }
        }
        logger.info("Found primary country " + primaryCountry);
        // return results
        if (primaryCountry != null) {
            results.add(new FocusLocation(
                    CountryGeoNameLookup.lookup(primaryCountry.name(), configuration, metricRegistry), countryCounts.get(primaryCountry))
            );
            for (CountryCode countryCode : countryCounts.keySet()) {
                if (countryCode != primaryCountry && countryCounts.get(countryCode) == countryCounts.get(primaryCountry)) {
                    results.add(new FocusLocation(
                            CountryGeoNameLookup.lookup(countryCode.name(), configuration, metricRegistry), countryCounts.get(countryCode))
                    );
                }
            }
        }
        return results;
    }

    @Override
    public List<FocusLocation> selectStates(List<ResolvedLocation> resolvedLocations) {
        List<FocusLocation> results = new ArrayList<FocusLocation>();
        // count state mentions
        HashMap<String, Integer> stateCounts = FocusUtils.getStateCounts(resolvedLocations, configuration, metricRegistry);
        if (stateCounts.size() == 0) {
            return results;
        }
        // find the most mentioned
        String primaryState = null;
        int highestCount = 0;
        for (String stateCode : stateCounts.keySet()) {
            int count = stateCounts.get(stateCode);
            if ((primaryState == null) || count > highestCount) {
                highestCount = count;
                primaryState = stateCode;
            }
        }
        logger.info("Found primary state " + primaryState.toString());
        // return results
        if (primaryState != null) {
            int primaryStateCount = stateCounts.get(primaryState);
            results.add(new FocusLocation(
                    Adm1GeoNameLookup.lookup(primaryState, configuration, metricRegistry), primaryStateCount));
            for (String stateCode : stateCounts.keySet()) {
                int count = stateCounts.get(stateCode);
                if (stateCode != primaryState && count == primaryStateCount) {
                    results.add(new FocusLocation(
                            Adm1GeoNameLookup.lookup(stateCode, configuration, metricRegistry), count));
                }
            }
        }
        return results;
    }

    @Override
    public List<FocusLocation> selectCities(List<ResolvedLocation> resolvedLocations) {
        List<FocusLocation> results = new ArrayList<FocusLocation>();
        // count state mentions
        HashMap<GeoName, Integer> cityCounts = FocusUtils.getCityCounts(resolvedLocations);
        if (cityCounts.size() == 0) {
            return results;
        }
        // find the most mentioned
        GeoName primaryCity = null;
        int highestCount = 0;
        for (GeoName geoname : cityCounts.keySet()) {
            int count = cityCounts.get(geoname);
            if ((primaryCity == null) || count > highestCount) {
                highestCount = count;
                primaryCity = geoname;
            }
        }
        logger.info("Found primary city " + primaryCity.toString());
        // return results
        if (primaryCity != null) {
            int primaryCityCount = cityCounts.get(primaryCity);
            results.add(new FocusLocation(primaryCity, primaryCityCount));
            for (GeoName city : cityCounts.keySet()) {
                int count = cityCounts.get(city);
                if ((city != primaryCity && count == primaryCityCount) || ((city != primaryCity && count > 1))) {
                    results.add(new FocusLocation(city, count));
                }
            }
        }
        return results;
    }

}
