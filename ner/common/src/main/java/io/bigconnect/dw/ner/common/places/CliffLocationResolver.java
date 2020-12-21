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

import com.bericotech.clavin.ClavinException;
import com.bericotech.clavin.extractor.LocationOccurrence;
import com.bericotech.clavin.gazetteer.CountryCode;
import com.bericotech.clavin.gazetteer.FeatureClass;
import com.bericotech.clavin.gazetteer.FeatureCode;
import com.bericotech.clavin.gazetteer.GeoName;
import com.bericotech.clavin.gazetteer.query.FuzzyMode;
import com.bericotech.clavin.gazetteer.query.Gazetteer;
import com.bericotech.clavin.gazetteer.query.QueryBuilder;
import com.bericotech.clavin.resolver.ClavinLocationResolver;
import com.bericotech.clavin.resolver.ResolvedLocation;
import io.bigconnect.dw.ner.common.places.disambiguation.HeuristicDisambiguationStrategy;
import io.bigconnect.dw.ner.common.places.disambiguation.LocationDisambiguationStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Override the deafult location resolving to include demonyms and use our disambiguation.
 *
 * @author rahulb
 */
public class CliffLocationResolver extends ClavinLocationResolver {

    private static final Logger logger = LoggerFactory.getLogger(CliffLocationResolver.class);

    public static final int MAX_HIT_DEPTH = 10;

    // my custom wrapper to let us try out multiple different disambiguation strategies
    private LocationDisambiguationStrategy disambiguationStrategy;

    private boolean filterOutDemonyms = false;

    public CliffLocationResolver(Gazetteer gazetteer) {
        super(gazetteer);
        disambiguationStrategy = new HeuristicDisambiguationStrategy();
    }

    public GeoName getByGeoNameId(int geoNameId) throws UnknownGeoNameIdException {
        try {
            return getGazetteer().getGeoName(geoNameId);
        } catch (ClavinException ce) {
            throw new UnknownGeoNameIdException(geoNameId);
        }
    }

    /**
     * Resolves the supplied list of location names into
     * {@link ResolvedLocation}s containing {@link GeoName} objects.
     * <p>
     * Calls {@link Gazetteer#getClosestLocations} on
     * each location name to find all possible matches, then uses
     * heuristics to select the best match for each by calling
     *
     * @param locations        list of location names to be resolved
     * @param maxHitDepth      number of candidate matches to consider
     * @param maxContextWindow how much context to consider when resolving
     * @param fuzzy            switch for turning on/off fuzzy matching
     * @return list of {@link ResolvedLocation} objects
     * @throws ClavinException if an error occurs parsing the search terms
     **/
    @SuppressWarnings("unchecked")
    public List<ResolvedLocation> resolveLocations(final List<LocationOccurrence> locations, final int maxHitDepth,
                                                   final int maxContextWindow, final boolean fuzzy) throws ClavinException {
        // are you forgetting something? -- short-circuit if no locations were provided
        if (locations == null || locations.isEmpty()) {
            return Collections.EMPTY_LIST;
        }

        // RB: turn off demonym filtering because we want those
        List<LocationOccurrence> filteredLocations;
        if (filterOutDemonyms) {
            /* Various named entity recognizers tend to mistakenly extract demonyms
             * (i.e., names for residents of localities (e.g., American, British))
             * as place names, which tends to gum up the works, so we make sure to
             * filter them out from the list of {@link LocationOccurrence}s passed
             * to the resolver.
             */
            filteredLocations = new ArrayList<LocationOccurrence>();
            for (LocationOccurrence location : locations)
                if (!isDemonym(location))
                    filteredLocations.add(location);
        } else {
            filteredLocations = locations;
        }
        // did we filter *everything* out?
        if (filteredLocations.isEmpty()) {
            return Collections.EMPTY_LIST;
        }

        QueryBuilder builder = new QueryBuilder()
                .maxResults(maxHitDepth)
                // translate CLAVIN 1.x 'fuzzy' parameter into NO_EXACT or OFF; it isn't
                // necessary, or desirable to support FILL for the CLAVIN resolution algorithm
                .fuzzyMode(fuzzy ? FuzzyMode.NO_EXACT : FuzzyMode.OFF)
                .includeHistorical(true);

        if (maxHitDepth > 1) { // perform context-based heuristic matching
            // stores all possible matches for each location name
            List<List<ResolvedLocation>> allCandidates = new ArrayList<List<ResolvedLocation>>();

            long startTime = System.nanoTime();
            // loop through all the location names
            for (LocationOccurrence location : filteredLocations) {
                // get all possible matches
                List<ResolvedLocation> candidates = new ArrayList<>();
                if (getGazetteer() != null) {
                    candidates.addAll(getGazetteer().getClosestLocations(builder.location(location).build()));
                } else {
                    candidates.add(toResolvedLocation(location));
                }

                // if we found some possible matches, save them
                if (candidates.size() > 0) {
                    allCandidates.add(candidates);
                }
            }
            long gazetteerTime = System.nanoTime() - startTime;

            // initialize return object
            List<ResolvedLocation> bestCandidates = new ArrayList<ResolvedLocation>();

            //RB: use out heuristic disambiguation instead of the CLAVIN default
            startTime = System.nanoTime();
            bestCandidates = disambiguationStrategy.select(this, allCandidates);
            long disambiguationTime = System.nanoTime() - startTime;
            /*
            // split-up allCandidates into reasonably-sized chunks to
            // limit computational load when heuristically selecting
            // the best matches
            for (List<List<ResolvedLocation>> theseCandidates : ListUtils.chunkifyList(allCandidates, maxContextWindow)) {
                // select the best match for each location name based
                // based on heuristics
                bestCandidates.addAll(pickBestCandidates(theseCandidates));
            }
            */
            logger.debug("gazetterAndDisambiguation: " + gazetteerTime + " / " + disambiguationTime);

            return bestCandidates;
        } else { // use no heuristics, simply choose matching location with greatest population
            // initialize return object
            List<ResolvedLocation> resolvedLocations = new ArrayList<ResolvedLocation>();

            // stores possible matches for each location name
            List<ResolvedLocation> candidateLocations;

            // loop through all the location names
            for (LocationOccurrence location : filteredLocations) {
                // choose the top-sorted candidate for each individual
                // location name
                if (getGazetteer() != null) {
                    candidateLocations = getGazetteer().getClosestLocations(builder.location(location).build());
                } else {
                    candidateLocations = new ArrayList<>();
                    candidateLocations.add(toResolvedLocation(location));
                }

                // if a match was found, add it to the return list
                if (candidateLocations.size() > 0) {
                    resolvedLocations.add(candidateLocations.get(0));
                }
            }

            return resolvedLocations;
        }
    }

    private ResolvedLocation toResolvedLocation(LocationOccurrence location) {
        return new ResolvedLocation(location, new DummyGeoName(location), location.getText(), false);
    }

    public void logStats() {
        disambiguationStrategy.logStats();
    }

    public static class DummyGeoName implements GeoName {
        private LocationOccurrence locationOccurrence;

        public DummyGeoName(LocationOccurrence locationOccurrence) {
            this.locationOccurrence = locationOccurrence;
        }

        @Override
        public String getPrimaryCountryName() {
            return "";
        }

        @Override
        public String getParentAncestryKey() {
            return null;
        }

        @Override
        public String getAncestryKey() {
            return null;
        }

        @Override
        public boolean isTopLevelAdminDivision() {
            return false;
        }

        @Override
        public boolean isTopLevelTerritory() {
            return false;
        }

        @Override
        public boolean isDescendantOf(GeoName geoname) {
            return false;
        }

        @Override
        public boolean isAncestorOf(GeoName geoname) {
            return false;
        }

        @Override
        public Integer getParentId() {
            return null;
        }

        @Override
        public GeoName getParent() {
            return null;
        }

        @Override
        public boolean setParent(GeoName prnt) {
            return false;
        }

        @Override
        public boolean isAncestryResolved() {
            return false;
        }

        @Override
        public int getGeonameID() {
            return 0;
        }

        @Override
        public String getName() {
            return locationOccurrence.getText();
        }

        @Override
        public String getAsciiName() {
            return getName();
        }

        @Override
        public List<String> getAlternateNames() {
            return new ArrayList<>();
        }

        @Override
        public String getPreferredName() {
            return getName();
        }

        @Override
        public double getLatitude() {
            return 0;
        }

        @Override
        public double getLongitude() {
            return 0;
        }

        @Override
        public FeatureClass getFeatureClass() {
            return FeatureClass.NULL;
        }

        @Override
        public FeatureCode getFeatureCode() {
            return FeatureCode.NULL;
        }

        @Override
        public CountryCode getPrimaryCountryCode() {
            return CountryCode.NULL;
        }

        @Override
        public List<CountryCode> getAlternateCountryCodes() {
            return new ArrayList<>();
        }

        @Override
        public String getAdmin1Code() {
            return null;
        }

        @Override
        public String getAdmin2Code() {
            return null;
        }

        @Override
        public String getAdmin3Code() {
            return null;
        }

        @Override
        public String getAdmin4Code() {
            return null;
        }

        @Override
        public long getPopulation() {
            return 0;
        }

        @Override
        public int getElevation() {
            return 0;
        }

        @Override
        public int getDigitalElevationModel() {
            return 0;
        }

        @Override
        public TimeZone getTimezone() {
            return null;
        }

        @Override
        public Date getModificationDate() {
            return null;
        }

        @Override
        public String getGazetteerRecord() {
            return null;
        }

        @Override
        public String getGazetteerRecordWithAncestry() {
            return null;
        }
    }
}
