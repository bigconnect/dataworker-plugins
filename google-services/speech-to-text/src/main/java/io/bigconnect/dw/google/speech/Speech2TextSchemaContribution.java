package io.bigconnect.dw.google.speech;

import com.mware.core.model.clientapi.dto.PropertyType;
import com.mware.core.model.properties.types.BooleanSingleValueBcProperty;
import com.mware.core.model.schema.SchemaConstants;
import com.mware.core.model.schema.SchemaContribution;
import com.mware.core.model.schema.SchemaFactory;
import com.mware.ge.TextIndexHint;

import java.util.EnumSet;

public class Speech2TextSchemaContribution implements SchemaContribution {
    public static final BooleanSingleValueBcProperty GOOGLE_S2T_PROPERTY = new BooleanSingleValueBcProperty("GS2T");
    public static final BooleanSingleValueBcProperty GOOGLE_S2T_PROGRESS_PROPERTY = new BooleanSingleValueBcProperty("GS2TProgress");

    @Override
    public boolean patchApplied(SchemaFactory schemaFactory) {
        return schemaFactory.getProperty(GOOGLE_S2T_PROPERTY.getPropertyName()) != null;
    }

    @Override
    public void patchSchema(SchemaFactory schemaFactory) {
        schemaFactory.newConceptProperty()
                .concepts(schemaFactory.getConcept(SchemaConstants.CONCEPT_TYPE_THING))
                .name(GOOGLE_S2T_PROPERTY.getPropertyName())
                .type(PropertyType.BOOLEAN)
                .textIndexHints(EnumSet.of(TextIndexHint.EXACT_MATCH))
                .userVisible(false)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(schemaFactory.getConcept(SchemaConstants.CONCEPT_TYPE_THING))
                .name(GOOGLE_S2T_PROGRESS_PROPERTY.getPropertyName())
                .type(PropertyType.BOOLEAN)
                .textIndexHints(EnumSet.of(TextIndexHint.EXACT_MATCH))
                .userVisible(false)
                .save();
    }
}
