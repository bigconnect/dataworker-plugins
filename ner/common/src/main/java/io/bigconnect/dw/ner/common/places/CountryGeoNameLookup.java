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

import com.bericotech.clavin.gazetteer.GeoName;
import com.mware.core.config.Configuration;
import io.bigconnect.dw.ner.common.ParseManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * If you have a country code (iso3166-alpha2) this returns the GeoName object for it
 */
public class CountryGeoNameLookup extends AbstractGeoNameLookup {

    public final static Logger logger = LoggerFactory.getLogger(CountryGeoNameLookup.class);

    public static final String RESOURCE_NAME = "countryInfo.txt";

    private static CountryGeoNameLookup instance;
    private Configuration configuration;

    public CountryGeoNameLookup(Configuration configuration) throws IOException {
        super();
        this.configuration = configuration;
    }

    @Override
    public void parse() {
        try {
            CliffLocationResolver resolver = ParseManager.getResolver(configuration);
            BufferedReader br = new BufferedReader(new InputStreamReader(this.getClass().getClassLoader().getResourceAsStream(RESOURCE_NAME)));
            String line = null;
            while ((line = br.readLine()) != null) {
                if(line.trim().length()==0){
                    continue;
                }
                if (line.charAt(0)=='#') {
                    continue;
                }
                String[] columns = line.trim().split("\t");
                try {
                    String iso3166Alpha2 = columns[0];
                    //String name = columns[4];
                    int geonameId = Integer.parseInt(columns[16]);
                    this.put(iso3166Alpha2, resolver.getByGeoNameId(geonameId));
                } catch (NumberFormatException nfe){
                    logger.error("Couldn't parse geoname id from line: "+line);
                } catch (UnknownGeoNameIdException ugie) {
                    logger.error("Uknown geoNameId "+ugie.getGeoNameId()+" for: "+columns[4]);
                }
            }
            logger.info("Loaded "+this.size()+" countries");
        } catch(Exception e){
            logger.error("Unable to load location resolver");
            logger.error(e.toString());
        }
    }

    private static CountryGeoNameLookup getInstance(Configuration configuration) throws IOException{
        if(instance==null){
            instance = new CountryGeoNameLookup(configuration);
        }
        return instance;
    }

    public static GeoName lookup(String countryCodeAlpha2, Configuration configuration) {
        try{
            CountryGeoNameLookup lookup = getInstance(configuration);
            GeoName countryGeoName = lookup.get(countryCodeAlpha2);
            logger.debug("Found '"+countryCodeAlpha2+"': "+countryGeoName);
            return countryGeoName;
        } catch (IOException ioe){
            logger.error("Couldn't lookup country geoname!");
            logger.error(ioe.toString());
        }
        return null;
    }

}
