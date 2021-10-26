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
package io.bigconnect.dw.mimetype.mapper;

import com.mware.core.exception.BcException;
import com.mware.core.ingest.dataworker.DataWorker;
import com.mware.core.ingest.dataworker.DataWorkerData;
import com.mware.core.ingest.dataworker.DataWorkerPrepareData;
import com.mware.core.model.Description;
import com.mware.core.model.Name;
import com.mware.core.model.properties.BcSchema;
import com.mware.core.model.properties.types.BcPropertyUpdate;
import com.mware.core.model.properties.types.StringSingleValueBcProperty;
import com.mware.core.model.schema.Concept;
import com.mware.core.model.schema.SchemaConstants;
import com.mware.core.model.schema.SchemaRepository;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.ge.Element;
import com.mware.ge.Property;
import com.mware.ge.Vertex;
import com.mware.ge.mutation.VertexMutation;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

@Name("MIME Type Ontology Mapper")
@Description("Maps MIME types to an ontology class")
public class MimeTypeOntologyMapperWorker extends DataWorker {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(MimeTypeOntologyMapperWorker.class);
    public static final String DEFAULT_MAPPING_KEY = "default";
    public static final String MAPPING_INTENT_KEY = "intent";
    public static final String MAPPING_IRI_KEY = "iri";
    public static final String MAPPING_REGEX_KEY = "regex";
    private Concept defaultConcept;
    private List<MimeTypeMatcher> mimeTypeMatchers = new ArrayList<>();

    @Override
    public void prepare(DataWorkerPrepareData workerPrepareData) throws Exception {
        super.prepare(workerPrepareData);
        loadMappings();
        logMappings();
    }

    private void logMappings() {
        for (MimeTypeMatcher matcher : mimeTypeMatchers) {
            LOGGER.debug("Matcher: %s", matcher.toString());
        }
        if (defaultConcept == null) {
            LOGGER.debug("No default concept");
        } else {
            LOGGER.debug("Default concept: %s", defaultConcept);
        }
    }

    private void loadMappings() {
        Map<String, Map<String, String>> mappings = getConfiguration().getMultiValue(MimeTypeOntologyMapperWorker.class.getName() + ".mapping");
        for (Map.Entry<String, Map<String, String>> mapping : mappings.entrySet()) {
            Concept concept = getConceptFromMapping(mapping.getValue());
            if (DEFAULT_MAPPING_KEY.equals(mapping.getKey())) {
                defaultConcept = concept;
                continue;
            }

            String regex = mapping.getValue().get(MAPPING_REGEX_KEY);
            if (regex != null) {
                mimeTypeMatchers.add(new RegexMimeTypeMatcher(concept, regex));
                continue;
            }

            throw new BcException("Expected mapping name of " + DEFAULT_MAPPING_KEY + " or a " + MAPPING_REGEX_KEY);
        }
    }

    private Concept getConceptFromMapping(Map<String, String> mapping) {
        String intent = mapping.get(MAPPING_INTENT_KEY);
        if (intent != null) {
            return getSchemaRepository().getRequiredConceptByIntent(intent, SchemaRepository.PUBLIC);
        }

        String iri = mapping.get(MAPPING_IRI_KEY);
        if (iri != null) {
            return getSchemaRepository().getRequiredConceptByName(iri, SchemaRepository.PUBLIC);
        }

        throw new BcException("Missing concept for mapping. Must specify " + MAPPING_INTENT_KEY + " or " + MAPPING_IRI_KEY + ".");
    }

    @Override
    public void execute(InputStream in, DataWorkerData data) throws Exception {
        String mimeType = BcSchema.MIME_TYPE.getOnlyPropertyValue(data.getElement());
        Concept concept = null;

        for (MimeTypeMatcher matcher : this.mimeTypeMatchers) {
            if (matcher.matches(mimeType)) {
                concept = matcher.getConcept();
                break;
            }
        }

        if (concept == null) {
            concept = defaultConcept;
        }

        if (concept == null) {
            LOGGER.debug("skipping, no concept mapped for vertex " + data.getElement().getId());
            return;
        }

        LOGGER.debug("assigning concept type %s to vertex %s", concept.getName(), data.getElement().getId());

        List<BcPropertyUpdate> changedProperties = new ArrayList<>();
        changedProperties.add(new BcPropertyUpdate(new StringSingleValueBcProperty("conceptType")));
        Vertex element = (Vertex) refresh(data.getElement());
        VertexMutation m = element.prepareMutation();
        m.alterConceptType(concept.getName());
        element = m.save(getAuthorizations());
        getGraph().flush();

        getWebQueueRepository().broadcastPropertyChange(element, "", "conceptType", data.getWorkspaceId());

        getWorkQueueRepository().pushOnDwQueue(
                element,
                changedProperties,
                data.getWorkspaceId(),
                data.getVisibilitySource(),
                data.getPriority()
        );
    }

    @Override
    public boolean isHandled(Element element, Property property) {
        if (property == null) {
            return false;
        }

        if (!property.getName().equals(BcSchema.MIME_TYPE.getPropertyName())) {
            return false;
        }

        if (!BcSchema.MIME_TYPE.hasProperty(element)) {
            return false;
        }

        if (!isVertex(element)) {
            return false;
        }

        Vertex v = (Vertex) element;
        return SchemaConstants.CONCEPT_TYPE_THING.equals(v.getConceptType());
    }

    private static abstract class MimeTypeMatcher {
        private final Concept concept;

        MimeTypeMatcher(Concept concept) {
            this.concept = concept;
        }

        Concept getConcept() {
            return concept;
        }

        public abstract boolean matches(String mimeType);
    }

    private static class RegexMimeTypeMatcher extends MimeTypeMatcher {
        private final Pattern regex;

        RegexMimeTypeMatcher(Concept concept, String regex) {
            super(concept);
            this.regex = Pattern.compile(regex);
        }

        @Override
        public boolean matches(String mimeType) {
            return regex.matcher(mimeType).matches();
        }

        @Override
        public String toString() {
            return "RegexMimeTypeMatcher{" +
                    "concept=" + getConcept() +
                    ", regex=" + regex +
                    '}';
        }
    }
}
