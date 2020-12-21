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
package io.bigconnect.dw.ner.regex;

import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import com.google.inject.Inject;
import com.mware.core.ingest.dataworker.DataWorker;
import com.mware.core.ingest.dataworker.DataWorkerData;
import com.mware.core.ingest.dataworker.DataWorkerPrepareData;
import com.mware.core.ingest.dataworker.RegexDataWorker;
import com.mware.core.model.Description;
import com.mware.core.model.Name;
import com.mware.core.model.clientapi.dto.VisibilityJson;
import com.mware.core.model.properties.BcSchema;
import com.mware.core.model.regex.Regex;
import com.mware.core.model.regex.RegexRepository;
import com.mware.core.model.schema.SchemaConstants;
import com.mware.core.model.termMention.TermMentionBuilder;
import com.mware.core.model.termMention.TermMentionUtils;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.ge.*;
import com.mware.ge.mutation.ElementMutation;
import com.mware.ge.values.storable.Values;
import com.mware.ontology.IgnoredMimeTypes;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


@Name("Regex Extractor")
@Description("Extracts entities from text based on regexes")
public class RegexExtractorWorker extends DataWorker {
    private final RegexRepository regexRepository;
    private TermMentionUtils termMentionUtils;

    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(RegexDataWorker.class);

    @Inject
    public RegexExtractorWorker(RegexRepository regexRepository) {
        this.regexRepository = regexRepository;

    }

    @Override
    public void prepare(DataWorkerPrepareData workerPrepareData) throws Exception {
        super.prepare(workerPrepareData);
        LOGGER.debug("Extractor prepared ");
        this.termMentionUtils = new TermMentionUtils(getGraph(), getVisibilityTranslator(), getAuthorizations(), getUser());
    }

    @Override
    public void execute(InputStream in, DataWorkerData data) throws Exception {
        Iterable<Regex> regexes = regexRepository.getAllRegexes();
        Vertex outVertex = (Vertex) data.getElement();
        List<Vertex> termMentions = new ArrayList<>();
        final String text = CharStreams.toString(new InputStreamReader(in, Charsets.UTF_8));

        Iterator<Regex> regexIterator = regexes.iterator();
        while (regexIterator.hasNext()) {
            Regex currentRegex = regexIterator.next();

            Pattern pattern = Pattern.compile(currentRegex.getPattern());
            Matcher matcher = pattern.matcher(text);

            while (matcher.find()) {
                String patternGroup = matcher.group();
                int start = matcher.start();
                int end = matcher.end();

                termMentions.add(termMentionUtils.createTermMention(
                        outVertex,
                        data.getProperty().getKey(),
                        data.getProperty().getName(),
                        patternGroup,
                        currentRegex.getConcept(),
                        start,
                        end,
                        data.getElementVisibilityJson()));
            }
        }

        termMentionUtils.resolveTermMentions(outVertex, termMentions);
        applyTermMentionFilters(outVertex, termMentions);
        pushTextUpdated(data);
        getGraph().flush();
    }

    @Override
    public boolean isHandled(Element element, Property property) {
        if (property == null) {
            return false;
        }

        if (property.getName().equals(BcSchema.RAW.getPropertyName())) {
            return false;
        }

        String mimeType = BcSchema.MIME_TYPE_METADATA.getMetadataValue(property.getMetadata(), null);

        return !(mimeType == null || !mimeType.startsWith("text")) &&
                !IgnoredMimeTypes.contains(BcSchema.MIME_TYPE.getFirstPropertyValue(element));
    }

    public void resolveTermMentions(Vertex outVertex, List<Vertex> termMentions) {
        VisibilityJson visibilityJson = new VisibilityJson();
        visibilityJson.setSource("");

        for (Vertex termMention : termMentions) {
            String conceptType = BcSchema.TERM_MENTION_CONCEPT_TYPE.getPropertyValue(termMention);
            String tmTitle = BcSchema.TERM_MENTION_TITLE.getPropertyValue(termMention);
            VisibilityJson outVertexVisibilityJson = new VisibilityJson();
            Metadata metadata = Metadata.create();
            BcSchema.VISIBILITY_JSON_METADATA.setMetadata(metadata, outVertexVisibilityJson, getVisibilityTranslator().getDefaultVisibility());
            if (tmTitle == null) {
                continue;
            }
            Vertex resolvedToVertex = findExistingVertexWithTitle(tmTitle, getAuthorizations());
            if(resolvedToVertex == null) {
                ElementMutation<Vertex> vertexMutation = getGraph().prepareVertex(outVertex.getVisibility(), conceptType);
                BcSchema.TITLE.addPropertyValue(vertexMutation, "NLP", tmTitle, metadata, outVertex.getVisibility());
                resolvedToVertex = vertexMutation.save(getAuthorizations());
            }

            Edge resolvedEdge = getGraph().prepareEdge(outVertex, resolvedToVertex, SchemaConstants.EDGE_LABEL_HAS_ENTITY, outVertex.getVisibility()).save(getAuthorizations());

            String processId = getClass().getName();
            new TermMentionBuilder(termMention, outVertex)
                    .resolvedTo(resolvedToVertex, resolvedEdge)
                    .title(tmTitle)
                    .conceptName(conceptType)
                    .process(processId)
                    .resolvedFromTermMention(null)
                    .visibilityJson(BcSchema.TERM_MENTION_VISIBILITY_JSON.getPropertyValue(termMention, new VisibilityJson()))
                    .save(getGraph(), getVisibilityTranslator(), getUser(), getAuthorizations());
        }
    }

    private Vertex findExistingVertexWithTitle(String title, Authorizations authorizations) {
        Iterator<Vertex> existingVertices = getGraph().query(authorizations)
                .has(BcSchema.TITLE.getPropertyName(), Values.stringValue(title))
                .vertices()
                .iterator();
        if (existingVertices.hasNext()) {
            return existingVertices.next();
        }
        return null;
    }
}
