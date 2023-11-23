package io.bigconnect.dw.text.summary;

import com.mware.core.model.clientapi.dto.PropertyType;
import com.mware.core.model.properties.types.StringSingleValueBcProperty;
import com.mware.core.model.schema.SchemaContribution;
import com.mware.core.model.schema.SchemaFactory;
import com.mware.ge.TextIndexHint;

public class SummarySchemaContribution implements SchemaContribution {
    public static final StringSingleValueBcProperty SUMMARY = new StringSingleValueBcProperty("summary");

    @Override
    public boolean patchApplied(SchemaFactory schemaFactory) {
        return schemaFactory.getProperty(SUMMARY.getPropertyName()) != null;
    }

    @Override
    public void patchSchema(SchemaFactory schemaFactory) {
        if (schemaFactory.getProperty(SUMMARY.getPropertyName()) == null) {
            schemaFactory.newConceptProperty()
                    .concepts(schemaFactory.getOrCreateThingConcept())
                    .displayName("Summary")
                    .name(SUMMARY.getPropertyName())
                    .userVisible(true)
                    .searchable(true)
                    .type(PropertyType.STRING)
                    .textIndexHints(TextIndexHint.FULL_TEXT)
                    .save();
        }
    }
}
