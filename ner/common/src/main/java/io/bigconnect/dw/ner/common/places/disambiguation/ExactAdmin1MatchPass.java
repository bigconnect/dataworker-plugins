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

import com.bericotech.clavin.gazetteer.FeatureCode;
import com.bericotech.clavin.resolver.ResolvedLocation;

import java.util.ArrayList;
import java.util.List;

public class ExactAdmin1MatchPass extends GenericPass {

    @Override
    protected List<List<ResolvedLocation>> disambiguate(
            List<List<ResolvedLocation>> possibilitiesToDo,
            List<ResolvedLocation> bestCandidates) {
        List<List<ResolvedLocation>> possibilitiesToRemove = new ArrayList<List<ResolvedLocation>>();
        for( List<ResolvedLocation> candidates: possibilitiesToDo){
            if(containsPopulatedCityExactMatch(candidates)){
                continue;
            }
            List<ResolvedLocation> exactMatchCandidates = getExactMatchesOrAdmin1ExactMatches(candidates);
            if(exactMatchCandidates.size() > 0) {
                ResolvedLocation firstCandidate = exactMatchCandidates.get(0);
                if(firstCandidate.getGeoname().getPopulation()>0 &&
                        firstCandidate.getGeoname().getFeatureCode().equals(FeatureCode.ADM1)){
                    bestCandidates.add(firstCandidate);
                    possibilitiesToRemove.add(candidates);
                }
            }
        }
        return possibilitiesToRemove;
    }

    /**
     * Tuned to skip tiny cities that are populated to solve the Oklahoma problem
     * and the Sao Paulo problem.  The population threshold is a subjective number based
     * on a number of specific test cases we have in the unit tests (from bug reports).
     * @param candidates
     * @return
     */
    private boolean containsPopulatedCityExactMatch(List<ResolvedLocation> candidates) {
        for(ResolvedLocation loc:candidates){
            if(loc.getGeoname().getPopulation()>300000 && isCity(loc) && isExactMatch(loc)){
                return true;
            }
        }
        return false;
    }

    @Override
    public String getDescription() {
        return "Pick states (Admin1) that are an exact match to help colocation step";
    }

}
