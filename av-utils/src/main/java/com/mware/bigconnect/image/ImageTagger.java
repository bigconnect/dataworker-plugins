package com.mware.bigconnect.image;

import com.mware.core.config.Configuration;
import com.mware.core.model.graph.GraphRepository;
import com.mware.core.model.properties.ArtifactDetectedObject;
import com.mware.core.model.properties.RawObjectSchema;
import com.mware.core.model.schema.SchemaRepository;
import com.mware.core.model.termMention.TermMentionRepository;
import com.mware.core.model.workQueue.WebQueueRepository;
import com.mware.core.model.workQueue.WorkQueueRepository;
import com.mware.core.security.DirectVisibilityTranslator;
import com.mware.ge.*;

public class ImageTagger {
    private final Graph graph;
    private final SchemaRepository schemaRepository;
    private final Authorizations authorizations;
    private final WorkQueueRepository workQueueRepository;
    private final GraphRepository graphRepository;

    public ImageTagger(
            Configuration configuration,
            Graph graph,
            SchemaRepository schemaRepository,
            Authorizations authorizations,
            WorkQueueRepository workQueueRepository,
            WebQueueRepository webQueueRepository,
            TermMentionRepository termMentionRepository
    ) {
        this.graph = graph;
        this.schemaRepository = schemaRepository;
        this.authorizations = authorizations;
        this.workQueueRepository = workQueueRepository;
        this.graphRepository = new GraphRepository(graph, new DirectVisibilityTranslator(), termMentionRepository,
                workQueueRepository, webQueueRepository, configuration);
    }

    public void createObject(
            Element node,
            Number x1,
            Number y1,
            Number x2,
            Number y2,
            Number score,
            String clazz
    ) {
        Vertex artifactVertex = graph.getVertex(node.getId(), authorizations);

        // Check for duplicates
        Iterable<Property> props = RawObjectSchema.DETECTED_OBJECT.getProperties(artifactVertex);
        for (Property prop : props) {
            ArtifactDetectedObject artifact = RawObjectSchema.DETECTED_OBJECT_METADATA.getMetadataValue(prop);
            // Duck typing
            if (artifact != null &&
                    Math.abs(artifact.getX1() - x1.doubleValue()) <= 0.01 &&
                    Math.abs(artifact.getX2() - x2.doubleValue()) <= 0.01 &&
                    Math.abs(artifact.getY1() - y1.doubleValue()) <= 0.01 &&
                    Math.abs(artifact.getY2() - y2.doubleValue()) <= 0.01 &&
                    clazz.equals(artifact.getConcept())) {
                return;
            }
        }

        // Create Artifact
        ArtifactDetectedObject artifactDetectedObject = new ArtifactDetectedObject(
                x1.doubleValue(),
                y1.doubleValue(),
                x2.doubleValue(),
                y2.doubleValue(),
                score.doubleValue(),
                clazz,
                "system",
                null,
                null,
                null
        );

        final String propertyKey = artifactDetectedObject.getMultivalueKey(ImageTagger.class.getName());
        Metadata m = Metadata.create();
        RawObjectSchema.DETECTED_OBJECT_METADATA.setMetadata(m, artifactDetectedObject, Visibility.EMPTY);
        RawObjectSchema.DETECTED_OBJECT.addPropertyValue(artifactVertex, propertyKey, artifactDetectedObject.getConcept(), m, Visibility.EMPTY, authorizations);

        graph.flush();
    }
}
