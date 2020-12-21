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
import com.bericotech.clavin.gazetteer.FeatureClass;
import com.bericotech.clavin.gazetteer.GeoName;
import com.bericotech.clavin.resolver.ResolvedLocation;
import com.mware.core.config.Configuration;
import io.bigconnect.dw.ner.common.places.Adm1GeoNameLookup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Set;

public class FocusUtils {

    private static final Logger logger = LoggerFactory.getLogger(FocusUtils.class);

    public static HashMap<GeoName, Integer> getCityCounts(List<ResolvedLocation> resolvedLocations) {
        HashMap<GeoName, Integer> cityCounts = new HashMap<GeoName, Integer>();
        for (ResolvedLocation resolvedLocation : resolvedLocations) {
            if (resolvedLocation.getGeoname().getFeatureClass() != FeatureClass.P) {
                continue;
            }
            Set<GeoName> cityCountKeys = cityCounts.keySet();
            boolean found = false;


            for (GeoName geoname : cityCountKeys) {
                if (geoname.getGeonameID() == resolvedLocation.getGeoname().getGeonameID()) {
                    cityCounts.put(geoname, cityCounts.get(geoname) + 1);
                    logger.debug("Adding count to city " + geoname.getAsciiName() + cityCounts.get(geoname));
                    found = true;
                    break;
                }
            }
            if (!found) {
                cityCounts.put(resolvedLocation.getGeoname(), 1);
                logger.debug("Adding city " + resolvedLocation.getGeoname().getAsciiName());
            }

        }
        return cityCounts;
    }

    public static HashMap<String, Integer> getStateCounts(List<ResolvedLocation> resolvedLocations, Configuration configuration) {
        HashMap<String, Integer> stateCounts = new HashMap<String, Integer>();
        for (ResolvedLocation resolvedLocation : resolvedLocations) {
            if (resolvedLocation.getGeoname().getPrimaryCountryCode() == CountryCode.NULL) {
                continue;
            }
            CountryCode country = resolvedLocation.getGeoname().getPrimaryCountryCode();
            String adm1Code = resolvedLocation.getGeoname().getAdmin1Code();
            String key = Adm1GeoNameLookup.getKey(country, adm1Code);
            if (!Adm1GeoNameLookup.isValid(key, configuration)) {    // skip things that aren't actually ADM1 codes
                continue;
            }
            if (!stateCounts.containsKey(key)) {
                stateCounts.put(key, 0);
            }
            stateCounts.put(key, stateCounts.get(key) + 1);
        }
        return stateCounts;
    }

    public static HashMap<CountryCode, Integer> getCountryCounts(List<ResolvedLocation> resolvedLocations) {
        HashMap<CountryCode, Integer> countryCounts = new HashMap<CountryCode, Integer>();
        for (ResolvedLocation resolvedLocation : resolvedLocations) {
            if (resolvedLocation.getGeoname().getPrimaryCountryCode() == CountryCode.NULL) {
                continue;
            }
            CountryCode country = resolvedLocation.getGeoname().getPrimaryCountryCode();
            if (!countryCounts.containsKey(country)) {
                countryCounts.put(country, 0);
            }
            countryCounts.put(country, countryCounts.get(country) + 1);
        }
        return countryCounts;
    }

    public static HashMap<String, Integer> getScoredStateCounts(List<ResolvedLocation> resolvedLocations, String text) {
        HashMap<String, Integer> stateCounts = new HashMap<String, Integer>();

        for (ResolvedLocation resolvedLocation : resolvedLocations) {
            if (resolvedLocation.getGeoname().getAdmin1Code() == null) {
                continue;
            }
            int position = resolvedLocation.getLocation().getPosition();
            int percent10 = text.length() / 10;

            int points = 1;
            if (position <= percent10) {
                points = 2;
            }

            String state = resolvedLocation.getGeoname().getAdmin1Code();
            if (!stateCounts.containsKey(state)) {
                stateCounts.put(state, 0);
            }
            stateCounts.put(state, stateCounts.get(state) + points);
        }
        return stateCounts;
    }
}
