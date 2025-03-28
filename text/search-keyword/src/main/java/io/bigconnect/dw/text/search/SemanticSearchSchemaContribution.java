package io.bigconnect.dw.text.search;

import com.mware.core.model.clientapi.dto.PropertyType;
import com.mware.core.model.properties.types.StringBcProperty;
import com.mware.core.model.schema.SchemaContribution;
import com.mware.core.model.schema.SchemaFactory;
import com.mware.ge.TextIndexHint;

public class SemanticSearchSchemaContribution implements SchemaContribution {
    // Define the properties as public static final so they can be imported in the worker
    public static final StringBcProperty OTHER_RELEVANT_KEYWORDS = new StringBcProperty("otherRelevantKeywords");
    public static final StringBcProperty OTHER_RELEVANT_BUCKETS = new StringBcProperty("otherRelevantBuckets");

    @Override
    public boolean patchApplied(SchemaFactory schemaFactory) {
        return schemaFactory.getProperty(OTHER_RELEVANT_KEYWORDS.getPropertyName()) != null;
    }

    @Override
    public void patchSchema(SchemaFactory schemaFactory) {
        if (schemaFactory.getProperty(OTHER_RELEVANT_KEYWORDS.getPropertyName()) == null) {
            schemaFactory.newConceptProperty()
                    .concepts(schemaFactory.getOrCreateThingConcept())
                    .displayName("Related Keywords")
                    .name(OTHER_RELEVANT_KEYWORDS.getPropertyName())
                    .userVisible(true)
                    .searchable(true)
                    .type(PropertyType.STRING)
                    .textIndexHints(TextIndexHint.FULL_TEXT, TextIndexHint.EXACT_MATCH)
                    .save();
        }

        if (schemaFactory.getProperty(OTHER_RELEVANT_BUCKETS.getPropertyName()) == null) {
            schemaFactory.newConceptProperty()
                    .concepts(schemaFactory.getOrCreateThingConcept())
                    .displayName("Related Buckets")
                    .name(OTHER_RELEVANT_BUCKETS.getPropertyName())
                    .userVisible(true)
                    .searchable(true)
                    .type(PropertyType.STRING)
                    .textIndexHints(TextIndexHint.FULL_TEXT, TextIndexHint.EXACT_MATCH)
                    .save();
        }
    }
}