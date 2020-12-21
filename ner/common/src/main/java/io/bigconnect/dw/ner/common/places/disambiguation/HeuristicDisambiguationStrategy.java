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
import io.bigconnect.dw.ner.common.places.CliffLocationResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Employ a variety of heuristics for picking the best candidate, based on what
 * might work better for news articles where we care about what _country_ is
 * being report on.
 *
 * This is originally modeled on the common colocation + cooccurance strategy.
 *
 * Failures I've noticed: Africa Del. "Rocky Mountains" Fla doesn't give you
 * Florida names ("Bristol Palin", "Chad")
 */
public class HeuristicDisambiguationStrategy implements LocationDisambiguationStrategy {

    private static final Logger logger = LoggerFactory
            .getLogger(HeuristicDisambiguationStrategy.class);

    private MultiplePassChain chain;    // keep this around so we can track stats

    public HeuristicDisambiguationStrategy() {
        // set up which passes and the order for disambiguating
        chain = new MultiplePassChain();
        chain.add(new LargeAreasPass());
        chain.add(new FuzzyMatchedCountriesPass());
        chain.add(new ExactAdmin1MatchPass());
        chain.add(new ExactColocationsPass());
        chain.add(new TopColocationsPass());
        chain.add(new TopAdminPopulatedPass());
        chain.add(new TopPreferringColocatedPass());
    }

    @Override
    public List<ResolvedLocation> select(CliffLocationResolver resolver, List<List<ResolvedLocation>> allPossibilities) {

        logger.debug("Starting with "+allPossibilities.size()+" lists to do:");
        // print all of them
        for( List<ResolvedLocation> candidates: allPossibilities){
            ResolvedLocation firstCandidate = candidates.get(0);
            logger.debug("  Location: "+firstCandidate.getLocation().getText()+"@"+firstCandidate.getLocation().getPosition());
            for( ResolvedLocation candidate: candidates){
                GenericPass.logResolvedLocationInfo(candidate);
            }
        }

        // all this does is run the chain we set up already
        List<ResolvedLocation> bestCandidates = chain.disambiguate(allPossibilities);

        return bestCandidates;
    }

    @Override
    public void logStats() {
        chain.logPassTriggerStats();
    }
}
