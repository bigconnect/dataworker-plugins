package io.bigconnect.dw.google.translate;

import com.mware.core.model.clientapi.dto.PropertyType;
import com.mware.core.model.properties.types.BooleanBcProperty;
import com.mware.core.model.properties.types.BooleanSingleValueBcProperty;
import com.mware.core.model.schema.SchemaConstants;
import com.mware.core.model.schema.SchemaContribution;
import com.mware.core.model.schema.SchemaFactory;
import com.mware.ge.TextIndexHint;

import java.util.EnumSet;

public class GoogleTranslateSchemaContribution implements SchemaContribution {
    public static final BooleanSingleValueBcProperty GOOGLE_TRANSLATE_PROPERTY = new BooleanSingleValueBcProperty("GTranslate");

    @Override
    public boolean patchApplied(SchemaFactory schemaFactory) {
        return schemaFactory.getProperty(GOOGLE_TRANSLATE_PROPERTY.getPropertyName()) != null;
    }

    @Override
    public void patchSchema(SchemaFactory schemaFactory) {
        schemaFactory.newConceptProperty()
                .concepts(schemaFactory.getConcept(SchemaConstants.CONCEPT_TYPE_THING))
                .name(GOOGLE_TRANSLATE_PROPERTY.getPropertyName())
                .type(PropertyType.BOOLEAN)
                .textIndexHints(EnumSet.of(TextIndexHint.EXACT_MATCH))
                .userVisible(false)
                .save();
    }
}
