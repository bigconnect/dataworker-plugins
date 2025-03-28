package io.bigconnect.dw.text.zeroShotClassification;

import com.mware.core.model.clientapi.dto.PropertyType;
import com.mware.core.model.properties.types.StringBcProperty;
import com.mware.core.model.schema.SchemaContribution;
import com.mware.core.model.schema.SchemaFactory;
import com.mware.ge.TextIndexHint;

public class ZeroShotClassificationSchemaContribution implements SchemaContribution {
    // Define the properties as public static final so they can be imported in the worker
    public static final String TOPIC_CLASSIFICATION_PROPERTY_PREFIX = "topicClassification";
    public static final String OTHER_RELEVANT_KEYWORDS_PROPERTY = "otherRelevantKeywords";

    @Override
    public boolean patchApplied(SchemaFactory schemaFactory) {
        return schemaFactory.getProperty(TOPIC_CLASSIFICATION_PROPERTY_PREFIX + 1) != null;
    }

    @Override
    public void patchSchema(SchemaFactory schemaFactory) {
        for (int i = 1; i <= 10; i++) {
            addTopicClassificationProperty(schemaFactory, new StringBcProperty(TOPIC_CLASSIFICATION_PROPERTY_PREFIX + i), "Topic Classification " + i);
        }
    }

    private void addTopicClassificationProperty(SchemaFactory schemaFactory, StringBcProperty property, String displayName) {
        if (schemaFactory.getProperty(property.getPropertyName()) == null) {
            schemaFactory.newConceptProperty()
                    .concepts(schemaFactory.getOrCreateThingConcept())
                    .displayName(displayName)
                    .name(property.getPropertyName())
                    .userVisible(true)
                    .searchable(true)
                    .type(PropertyType.STRING)
                    .textIndexHints(TextIndexHint.FULL_TEXT, TextIndexHint.EXACT_MATCH)
                    .save();
        }
    }
}