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

import com.bericotech.clavin.resolver.ResolvedLocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class ExactColocationsPass extends GenericPass {

    private static final Logger logger = LoggerFactory.getLogger(ExactColocationsPass.class);

    @Override
    protected List<List<ResolvedLocation>> disambiguate(
            List<List<ResolvedLocation>> possibilitiesToDo,
            List<ResolvedLocation> bestCandidates) {
        List<List<ResolvedLocation>> possibilitiesToRemove = new ArrayList<List<ResolvedLocation>>();
        possibilitiesToRemove.clear();

        if(bestCandidates.size() == 0){
            return possibilitiesToRemove;
        }

        for( List<ResolvedLocation> candidates: possibilitiesToDo){
            ResolvedLocation candidateToPick = null;
            List<ResolvedLocation> colocatedExactCityCandidates = inSameCountry(candidates, bestCandidates,true,true,true);
            logger.debug("  Found "+colocatedExactCityCandidates.size()+" colocations");
            if(colocatedExactCityCandidates.size()==1){
                candidateToPick = colocatedExactCityCandidates.get(0);
            }else if (colocatedExactCityCandidates.size()>1){
                List<ResolvedLocation> shareCountryAndAdm1 = inSameCountryAndAdm1(colocatedExactCityCandidates,bestCandidates);
                if(shareCountryAndAdm1.size()>0){
                    candidateToPick = shareCountryAndAdm1.get(0);
                } else {
                    candidateToPick = colocatedExactCityCandidates.get(0);
                }
            }
            if(candidateToPick!=null){
                logger.debug("  "+candidateToPick.getGeoname().getGeonameID()+"  "+candidateToPick.getGeoname().getName() + " is in "+candidateToPick.getGeoname().getPrimaryCountryCode());
                bestCandidates.add(candidateToPick);
                possibilitiesToRemove.add(candidates);
            }
        }

        return possibilitiesToRemove;
    }

    /**
     *
     * @param colocatedExactCityCandidates
     * @param alreadyPicked
     * @return
     */
    private List<ResolvedLocation> inSameCountryAndAdm1(
            List<ResolvedLocation> candidates,
            List<ResolvedLocation> alreadyPicked) {
        List<ResolvedLocation> colocations = new ArrayList<ResolvedLocation>();
        for(ResolvedLocation pickedLocation:alreadyPicked){
            for(ResolvedLocation candidate:candidates){
                if(isSameCountryAndAdm1(candidate, pickedLocation)){
                    colocations.add(candidate);
                }
            }
        }
        return colocations;
    }

    private boolean isSameCountryAndAdm1(ResolvedLocation place1, ResolvedLocation place2) {
        return place1.getGeoname().getAdmin1Code().equals(place2.getGeoname().getAdmin1Code());
    }

    @Override
    public String getDescription() {
        return "Looking for top populated city exact match in same countries/states as best results so far";
    }

}
