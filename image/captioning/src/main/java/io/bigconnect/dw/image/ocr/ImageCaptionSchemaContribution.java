package io.bigconnect.dw.image.ocr;

import com.mware.core.model.clientapi.dto.PropertyType;
import com.mware.core.model.properties.types.BooleanSingleValueBcProperty;
import com.mware.core.model.properties.types.StringSingleValueBcProperty;
import com.mware.core.model.schema.SchemaContribution;
import com.mware.core.model.schema.SchemaFactory;
import com.mware.ge.TextIndexHint;

public class ImageCaptionSchemaContribution implements SchemaContribution {
    public static final BooleanSingleValueBcProperty PERFORM_CAPTION = new BooleanSingleValueBcProperty("doCaption");
    public static final StringSingleValueBcProperty CAPTION = new StringSingleValueBcProperty("caption");

    @Override
    public boolean patchApplied(SchemaFactory schemaFactory) {
        return schemaFactory.getProperty(PERFORM_CAPTION.getPropertyName()) != null ||
                schemaFactory.getProperty(CAPTION.getPropertyName()) != null;
    }

    @Override
    public void patchSchema(SchemaFactory schemaFactory) {
        if (schemaFactory.getProperty(PERFORM_CAPTION.getPropertyName()) == null) {
            schemaFactory.newConceptProperty()
                    .concepts(schemaFactory.getOrCreateThingConcept())
                    .name(PERFORM_CAPTION.getPropertyName())
                    .userVisible(false)
                    .searchable(false)
                    .type(PropertyType.BOOLEAN)
                    .textIndexHints(TextIndexHint.NONE)
                    .save();
        }
        if (schemaFactory.getProperty(CAPTION.getPropertyName()) == null) {
            schemaFactory.newConceptProperty()
                    .concepts(schemaFactory.getOrCreateThingConcept())
                    .name(CAPTION.getPropertyName())
                    .displayName("Caption")
                    .userVisible(true)
                    .searchable(true)
                    .type(PropertyType.STRING)
                    .textIndexHints(TextIndexHint.FULL_TEXT)
                    .save();
        }
    }
}