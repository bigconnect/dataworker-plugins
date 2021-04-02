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
import com.google.inject.Inject;
import com.mware.core.ingest.dataworker.DataWorker;
import com.mware.core.ingest.dataworker.DataWorkerData;
import com.mware.core.ingest.dataworker.DataWorkerPrepareData;
import com.mware.core.model.clientapi.dto.VisibilityJson;
import com.mware.core.model.properties.BcSchema;
import com.mware.core.model.properties.RawObjectSchema;
import com.mware.core.model.schema.SchemaConstants;
import com.mware.core.model.termMention.TermMentionBuilder;
import com.mware.core.model.termMention.TermMentionRepository;
import com.mware.core.model.termMention.TermMentionUtils;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.ge.*;
import com.mware.ge.mutation.ElementMutation;
import com.mware.ge.query.Compare;
import com.mware.ge.query.QueryResultsIterable;
import com.mware.ge.values.storable.StreamingPropertyValue;
import com.mware.ge.values.storable.Values;
import com.mware.ontology.IgnoredMimeTypes;
import io.bigconnect.dw.ner.common.extractor.ExtractedEntities;
import io.bigconnect.dw.ner.common.extractor.GenericOccurrence;
import io.bigconnect.dw.ner.common.extractor.OrganizationOccurrence;
import io.bigconnect.dw.ner.common.extractor.PersonOccurrence;
import io.bigconnect.dw.ner.common.orgs.ResolvedOrganization;
import io.bigconnect.dw.ner.common.people.ResolvedPerson;
import io.bigconnect.dw.text.common.NerUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class EntityExtractionDataWorker extends DataWorker {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(EntityExtractionDataWorker.class);

    private TermMentionRepository termMentionRepository;
    private TermMentionUtils termMentionUtils;
    boolean resolveUnknownEntities;

    @Inject
    public EntityExtractionDataWorker(TermMentionRepository termMentionRepository) {
        this.termMentionRepository = termMentionRepository;
    }

    @Override
    public void prepare(DataWorkerPrepareData workerPrepareData) throws Exception {
        super.prepare(workerPrepareData);
        this.resolveUnknownEntities = getConfiguration().getBoolean("entity.extractor.resolve-unknown", false);
        this.termMentionUtils = new TermMentionUtils(getGraph(), getVisibilityTranslator(), getAuthorizations(), getUser());
    }

    @Override
    public boolean isHandled(Element element, Property property) {
        if (property == null) {
            return false;
        }

        if (IgnoredMimeTypes.contains(BcSchema.MIME_TYPE.getFirstPropertyValue(element)))
            return false;

        if (property.getName().equals(RawObjectSchema.RAW_LANGUAGE.getPropertyName())) {
            // do entity extraction only if language is set
            String language = RawObjectSchema.RAW_LANGUAGE.getPropertyValue(property);
            return !StringUtils.isEmpty(language);
        }

        return false;
    }

    @Override
    public void execute(InputStream in, DataWorkerData data) throws Exception {
        String language = RawObjectSchema.RAW_LANGUAGE.getPropertyValue(data.getProperty());
        Property textProperty = BcSchema.TEXT.getProperty(refresh(data.getElement()), data.getProperty().getKey());
        StreamingPropertyValue textPropertyValue = BcSchema.TEXT.getPropertyValue(textProperty);

        if (textPropertyValue == null) {
            LOGGER.warn("Could not find text property for language: "+language);
            return;
        }

        String text = IOUtils.toString(textPropertyValue.getInputStream(), StandardCharsets.UTF_8);

        if (StringUtils.isEmpty(text)) {
            return;
        }

        try {
            Vertex outVertex = (Vertex) refresh(data.getElement());

            NerUtils.removeTermMentions(outVertex, termMentionRepository, termMentionUtils, getGraph(), getAuthorizations());
            ExtractedEntities entities = ParseManager.extractAndResolve(getConfiguration(), language, text);
            if (entities != null) {
                VisibilityJson tmVisibilityJson = new VisibilityJson();
                tmVisibilityJson.setSource("");

                addLocations(outVertex, textProperty, tmVisibilityJson, entities);
                addPersons(outVertex, textProperty, tmVisibilityJson, entities);
                addOrganizations(outVertex, textProperty, tmVisibilityJson, entities);
                addOtherEntities(outVertex, textProperty, tmVisibilityJson, entities);
                getGraph().flush();

                pushTextUpdated(data);
            } else {
                LOGGER.debug("No entities extracted");
            }
        } catch (Exception e) {
            LOGGER.error("Error extracting entities: "+e.getMessage(), e);
        }
    }

    private List<Vertex> addLocations(Vertex outVertex, Property property, VisibilityJson visibilityJson, ExtractedEntities entities) {
        Set<String> alreadyResolvedMentions = new HashSet<>();
        List<Vertex> termMentions = new ArrayList<>();

        for (ResolvedLocation resolvedLocation : entities.getResolvedLocations()) {
            float confidence = resolvedLocation.getConfidence(); // low is good
            int start = resolvedLocation.getLocation().getPosition();
            int end = start + resolvedLocation.getLocation().getText().length();
            String name = resolvedLocation.getGeoname().getName();

            // create a single term mention for each resolved name
            if(alreadyResolvedMentions.contains(name))
                continue;

            Vertex termMention = termMentionUtils.createTermMention(
                    outVertex,
                    property.getKey(),
                    property.getName(),
                    name,
                    SchemaConstants.CONCEPT_TYPE_LOCATION,
                    start,
                    end,
                    visibilityJson
            );
            termMentions.add(termMention);

            Vertex resolvedToVertex = findExistingVertexWithConceptAndTitle(SchemaConstants.CONCEPT_TYPE_LOCATION, name);
            if (resolvedToVertex == null && resolveUnknownEntities) {
                resolvedToVertex = createResolvedVertex(SchemaConstants.CONCEPT_TYPE_LOCATION, name, outVertex.getVisibility());
                // Comentat pt ca se rezolva de serviciul de AI, prin pipelines
                //GeoPoint geoPoint = new GeoPoint(resolvedLocation.getGeoname().getLatitude(), resolvedLocation.getGeoname().getLongitude());
                //RawObjectSchema.GEOLOCATION_PROPERTY.addPropertyValue(resolvedToVertex, "", geoPoint, resolvedToVertex.getVisibility(), getAuthorizations());
            }

            if (resolvedToVertex != null) {
                resolveTermMention(outVertex, termMention, resolvedToVertex, SchemaConstants.CONCEPT_TYPE_LOCATION, name);
                getGraph().flush();
            }

            alreadyResolvedMentions.add(name);
        }

        return termMentions;
    }

    private List<Vertex> addOrganizations(Vertex outVertex, Property property, VisibilityJson visibilityJson, ExtractedEntities entities) {
        List<Vertex> termMentions = new ArrayList<>();
        Set<String> alreadyResolvedMentions = new HashSet<>();
        List<ResolvedOrganization> resolvedOrganizations = entities.getResolvedOrganizations();
        for (ResolvedOrganization organization : resolvedOrganizations) {
            String name = organization.getName();
            // resolve only the first occurence
            OrganizationOccurrence occurrence = organization.getOccurrences().get(0);
            int start = occurrence.position;
            int end = start + occurrence.text.length();

            // create a single term mention for each resolved name
            if(alreadyResolvedMentions.contains(name))
                continue;

            Vertex termMention = termMentionUtils.createTermMention(
                    outVertex,
                    property.getKey(),
                    property.getName(),
                    name,
                    SchemaConstants.CONCEPT_TYPE_ORGANIZATION,
                    start,
                    end,
                    visibilityJson
            );
            termMentions.add(termMention);

            Vertex resolvedToVertex = findExistingVertexWithConceptAndTitle(SchemaConstants.CONCEPT_TYPE_ORGANIZATION, name);
            if (resolvedToVertex == null && resolveUnknownEntities) {
                resolvedToVertex = createResolvedVertex(SchemaConstants.CONCEPT_TYPE_ORGANIZATION, name, outVertex.getVisibility());
            }

            if (resolvedToVertex != null) {
                resolveTermMention(outVertex, termMention, resolvedToVertex, SchemaConstants.CONCEPT_TYPE_ORGANIZATION, name);
                getGraph().flush();
            }

            alreadyResolvedMentions.add(name);
        }
        return termMentions;
    }

    private List<Vertex> addPersons(Vertex outVertex, Property property, VisibilityJson visibilityJson, ExtractedEntities entities) {
        Set<String> alreadyResolvedMentions = new HashSet<>();
        List<Vertex> termMentions = new ArrayList<>();
        List<ResolvedPerson> resolvedOrganizations = entities.getResolvedPeople();
        for (ResolvedPerson person : resolvedOrganizations) {
            String name = person.getName();
            // resolve only the first occurence
            PersonOccurrence occurrence = person.getOccurrences().get(0);
            int start = occurrence.position;
            int end = start + occurrence.text.length();

            // create a single term mention for each resolved name
            if(alreadyResolvedMentions.contains(name))
                continue;

            Vertex termMention = termMentionUtils.createTermMention(
                    outVertex,
                    property.getKey(),
                    property.getName(),
                    name,
                    SchemaConstants.CONCEPT_TYPE_PERSON,
                    start,
                    end,
                    visibilityJson
            );
            termMentions.add(termMention);

            Vertex resolvedToVertex = findExistingVertexWithConceptAndTitle(SchemaConstants.CONCEPT_TYPE_PERSON, name);
            if (resolvedToVertex == null && resolveUnknownEntities) {
                resolvedToVertex = createResolvedVertex(SchemaConstants.CONCEPT_TYPE_PERSON, name, outVertex.getVisibility());
            }

            if (resolvedToVertex != null) {
                resolveTermMention(outVertex, termMention, resolvedToVertex, SchemaConstants.CONCEPT_TYPE_PERSON, name);
                getGraph().flush();
            }

            alreadyResolvedMentions.add(name);
        }

        return termMentions;
    }

    private List<Vertex> addOtherEntities(Vertex outVertex, Property property, VisibilityJson visibilityJson, ExtractedEntities entities) {
        Set<String> alreadyResolvedMentions = new HashSet<>();
        List<Vertex> termMentions = new ArrayList<>();
        List<GenericOccurrence> otherEntities = entities.getOtherEntities();
        for (GenericOccurrence entity : otherEntities) {
            String name = entity.text;
            int start = entity.position;
            int end = start + name.length();
            String conceptType = entity.conceptType;

            // create a single term mention for each resolved name
            if(alreadyResolvedMentions.contains(name))
                continue;

            Vertex termMention = termMentionUtils.createTermMention(
                    outVertex,
                    property.getKey(),
                    property.getName(),
                    name,
                    conceptType,
                    start,
                    end,
                    visibilityJson
            );
            termMentions.add(termMention);

            Vertex resolvedToVertex = findExistingVertexWithConceptAndTitle(conceptType, name);
            if (resolvedToVertex == null && resolveUnknownEntities) {
                resolvedToVertex = createResolvedVertex(SchemaConstants.CONCEPT_TYPE_PERSON, name, outVertex.getVisibility());
            }

            if (resolvedToVertex != null) {
                resolveTermMention(outVertex, termMention, resolvedToVertex, conceptType, name);
                getGraph().flush();
            }
            alreadyResolvedMentions.add(name);
        }

        return termMentions;
    }

    private Vertex findExistingVertexWithConceptAndTitle(String conceptType, String title) {
        try (QueryResultsIterable<Vertex> existingVertices = getGraph().query(getAuthorizations())
                .hasConceptType(conceptType)
                .has(BcSchema.TITLE.getPropertyName(), Compare.EQUAL, Values.stringValue(title.trim()))
                .vertices()) {

            Iterator<Vertex> iterator = existingVertices.iterator();
            if (iterator.hasNext()) {
                return iterator.next();
            }
        } catch (IOException ex) {
            LOGGER.warn(ex.getMessage());
        }

        return null;
    }

    private Vertex resolveTermMention(Vertex outVertex, Vertex termMention, Vertex resolvedToVertex, String conceptType, String title) {
        String edgeId = outVertex.getId() + "-" + SchemaConstants.EDGE_LABEL_HAS_DETECTED_ENTITY + "-" + resolvedToVertex.getId();
        Edge resolvedEdge = getGraph().prepareEdge(edgeId, outVertex, resolvedToVertex, SchemaConstants.EDGE_LABEL_HAS_DETECTED_ENTITY, outVertex.getVisibility())
                .save(getAuthorizations());
        String processId = getClass().getName();

        new TermMentionBuilder(termMention, outVertex)
                .resolvedTo(resolvedToVertex, resolvedEdge)
                .title(title)
                .conceptName(conceptType)
                .process(processId)
                .resolvedFromTermMention(termMention.getId())
                .visibilityJson(BcSchema.TERM_MENTION_VISIBILITY_JSON.getPropertyValue(termMention, new VisibilityJson()))
                .save(getGraph(), getVisibilityTranslator(), getUser(), getAuthorizations());

        return resolvedToVertex;
    }

    private Vertex createResolvedVertex(String conceptType, String title, Visibility visibility) {
        VisibilityJson visibilityJson = new VisibilityJson();
        Metadata metadata = Metadata.create();
        BcSchema.VISIBILITY_JSON_METADATA.setMetadata(metadata, visibilityJson, getVisibilityTranslator().getDefaultVisibility());
        String id = getGraph().getIdGenerator().nextId();
        ElementMutation<Vertex> vertexMutation = getGraph().prepareVertex(id, visibility, conceptType);
        BcSchema.TITLE.addPropertyValue(vertexMutation, "", title, metadata, visibility);
        return vertexMutation.save(getAuthorizations());
    }
}
