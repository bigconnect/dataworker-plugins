
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
package io.bigconnect.dw.ner.common.places;

import com.bericotech.clavin.gazetteer.CountryCode;
import com.bericotech.clavin.gazetteer.GeoName;
import com.mware.core.config.Configuration;
import io.bigconnect.dw.ner.common.ParseManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * If you have a country code (iso3166-alpha2) and ADM1 code this returns the GeoName object for it.
 */
public class Adm1GeoNameLookup extends AbstractGeoNameLookup {

    public final static Logger logger = LoggerFactory.getLogger(Adm1GeoNameLookup.class);

    public static final String RESOURCE_NAME = "admin1CodesASCII.txt";

    private static Adm1GeoNameLookup instance;
    private Configuration configuration;

    public Adm1GeoNameLookup(Configuration configuration) throws IOException {
        super();
        this.configuration = configuration;
    }

    public static String getKey(String countryCode, String ADM1) {
        return countryCode + "." + ADM1;
    }

    public static String getKey(CountryCode countryCode, String ADM1) {
        return getKey(countryCode.name(), ADM1);
    }

    public GeoName get(String countryCode, String ADM1) {
        return this.get(getKey(countryCode, ADM1));
    }

    @Override
    public void parse() {
        try {
            CliffLocationResolver resolver = ParseManager.getResolver(configuration);
            BufferedReader br = new BufferedReader(new InputStreamReader(this.getClass().getClassLoader().getResourceAsStream(RESOURCE_NAME)));
            String line = null;
            while ((line = br.readLine()) != null) {
                if (line.trim().length() == 0) {
                    continue;
                }
                if (line.charAt(0) == '#') {
                    continue;
                }
                String[] columns = line.trim().split("\t");
                String key = columns[0];
                String name = columns[1];
                int geonameId = Integer.parseInt(columns[3]);
                try {
                    this.put(key, resolver.getByGeoNameId(geonameId));
                } catch (UnknownGeoNameIdException e) {
                    logger.error("Uknown geoNameId " + geonameId + " for " + name);
                }
            }
            logger.info("Loaded " + this.size() + " countries");
        } catch (Exception e) {
            logger.error("Unable to load location resolver");
            logger.error(e.toString());
        }
    }

    private static Adm1GeoNameLookup getInstance(Configuration configuration) throws IOException {
        if (instance == null) {
            instance = new Adm1GeoNameLookup(configuration);
        }
        return instance;
    }

    public static GeoName lookup(String countryCodeDotAdm1Code, Configuration configuration) {
        try {
            Adm1GeoNameLookup lookup = getInstance(configuration);
            GeoName geoName = lookup.get(countryCodeDotAdm1Code);
            logger.debug("Found '" + countryCodeDotAdm1Code + "': " + geoName);
            return geoName;
        } catch (IOException ioe) {
            logger.error("Couldn't lookup state ADM1 geoname!");
            logger.error(ioe.toString());
        }
        return null;
    }

    public static boolean isValid(String countryCodeDotAdm1Code, Configuration configuration) {
        boolean valid = false;
        try {
            Adm1GeoNameLookup lookup = getInstance(configuration);
            valid = lookup.contains(countryCodeDotAdm1Code);
        } catch (IOException ioe) {
            logger.error("Couldn't lookup state ADM1 geoname!");
            logger.error(ioe.toString());
        }
        return valid;
    }

    public static GeoName lookup(String countryCode, String adm1Code, Configuration configuration) {
        return lookup(getKey(countryCode, adm1Code), configuration);
    }

}
