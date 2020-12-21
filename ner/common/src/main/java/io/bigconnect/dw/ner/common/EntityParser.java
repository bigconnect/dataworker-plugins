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

import com.bericotech.clavin.resolver.ResolvedLocation;
import io.bigconnect.dw.ner.common.extractor.EntityExtractorService;
import io.bigconnect.dw.ner.common.extractor.ExtractedEntities;
import io.bigconnect.dw.ner.common.orgs.OrganizationResolver;
import io.bigconnect.dw.ner.common.orgs.ResolvedOrganization;
import io.bigconnect.dw.ner.common.people.PersonResolver;
import io.bigconnect.dw.ner.common.people.ResolvedPerson;
import io.bigconnect.dw.ner.common.places.CliffLocationResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Patterned after com.bericotech.clavin.GeoParser
 *
 * @author rahulb
 */
public class EntityParser {

    private static final Logger logger = LoggerFactory.getLogger(EntityParser.class);

    // entity stanford to find location names in text
    private EntityExtractorService extractor;

    private CliffLocationResolver locationResolver;
    private PersonResolver personResolver;
    private OrganizationResolver organizationResolver;

    // switch controlling use of fuzzy matching
    private final boolean fuzzy;
    private final int maxHitDepth;

    public EntityParser(
            EntityExtractorService extractor,
            CliffLocationResolver resolver,
            boolean fuzzy,
            int maxHitDepth
    ) {
        this.extractor = extractor;
        this.locationResolver = resolver;
        this.personResolver = new PersonResolver();
        this.organizationResolver = new OrganizationResolver();
        this.fuzzy = fuzzy;
        this.maxHitDepth = maxHitDepth;
    }

    public ExtractedEntities extractAndResolve(String languageCode, String inputText, boolean manuallyReplaceDemonyms) throws Exception {
        ExtractedEntities extractedEntities = extractor.extractEntities(languageCode, inputText, manuallyReplaceDemonyms);
        return resolve(extractedEntities);
    }

    @SuppressWarnings("rawtypes")
    public ExtractedEntities extractAndResolveFromSentences(String languageCode, Map[] sentences, boolean manuallyReplaceDemonyms) throws Exception {
        ExtractedEntities extractedEntities = extractor.extractEntitiesFromSentences(languageCode, sentences, manuallyReplaceDemonyms);
        return resolve(extractedEntities);
    }

    public ExtractedEntities resolve(ExtractedEntities entities) throws Exception {
        // resolve the extracted location names against a
        // gazetteer to produce geographic entities representing the
        // locations mentioned in the original text
        List<ResolvedLocation> resolvedLocations = locationResolver.resolveLocations(
                entities.getLocations(), this.maxHitDepth, -1, this.fuzzy
        );
        entities.setResolvedLocations(resolvedLocations);
        logger.trace("resolvedLocations: {}", resolvedLocations);

        // Disambiguate people
        List<ResolvedPerson> resolvedPeople = personResolver.resolve(entities.getPeople());
        entities.setResolvedPeople(resolvedPeople);
        logger.trace("resolvedPeople: {}", resolvedPeople);

        // Disambiguate organizations
        List<ResolvedOrganization> resolvedOrganizations = organizationResolver.resolve(entities.getOrganizations());
        entities.setResolvedOrganizations(resolvedOrganizations);
        logger.trace("resolvedOrganizations: {}", resolvedOrganizations);

        return entities;

    }

}
