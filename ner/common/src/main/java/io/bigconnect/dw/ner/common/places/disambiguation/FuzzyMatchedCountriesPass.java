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
package io.bigconnect.dw.ner.common.places.disambiguation;

import com.bericotech.clavin.gazetteer.FeatureClass;
import com.bericotech.clavin.resolver.ResolvedLocation;

import java.util.ArrayList;
import java.util.List;

public class FuzzyMatchedCountriesPass extends GenericPass {

    //private static final Logger logger = LoggerFactory.getLogger(FuzzyMatchedCountriesPass.class);

    @Override
    protected List<List<ResolvedLocation>> disambiguate(
            List<List<ResolvedLocation>> possibilitiesToDo,
            List<ResolvedLocation> bestCandidates) {
        List<List<ResolvedLocation>> possibilitiesToRemove = new ArrayList<List<ResolvedLocation>>();
        for( List<ResolvedLocation> candidates: possibilitiesToDo){
            ResolvedLocation countryCandidate = findFirstCountryCandidate(candidates,false);
            if (countryCandidate!=null){
                bestCandidates.add(countryCandidate);
                possibilitiesToRemove.add(candidates);
            }
        }
        return possibilitiesToRemove;
    }

    /**
     * Find the first country after any Hypsographic Features (T)
     * @param candidates
     * @param exactMatchRequired
     * @return
     */
    protected ResolvedLocation findFirstCountryCandidate(List<ResolvedLocation> candidates, boolean exactMatchRequired){
        boolean keepGoing = true;
        for(ResolvedLocation candidate: candidates) {
            if(keepGoing){
                if(candidate.getGeoname().getFeatureClass().equals(FeatureClass.T)){
                    // skip large territories that appear ahead of countries in results (ie. Indian Subcontinent!)
                    continue;
                }
                if(isCountry(candidate)){
                    if (exactMatchRequired && isExactMatch(candidate)){
                        return candidate;
                    } else if (!exactMatchRequired){
                        return candidate;
                    }
                } else{
                    keepGoing = false;
                }
            }
        }
        return null;
    }

    @Override
    public String getDescription() {
        return "Pick countries that might not be an exact match";
    }

}
