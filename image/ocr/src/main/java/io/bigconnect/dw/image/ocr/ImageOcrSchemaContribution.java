package io.bigconnect.dw.image.ocr;

import com.mware.core.model.clientapi.dto.PropertyType;
import com.mware.core.model.properties.types.BooleanSingleValueBcProperty;
import com.mware.core.model.schema.SchemaContribution;
import com.mware.core.model.schema.SchemaFactory;
import com.mware.ge.TextIndexHint;

public class ImageOcrSchemaContribution implements SchemaContribution {
    public static final BooleanSingleValueBcProperty PERFORM_OCR = new BooleanSingleValueBcProperty("ocr");

    @Override
    public boolean patchApplied(SchemaFactory schemaFactory) {
        return schemaFactory.getProperty(PERFORM_OCR.getPropertyName()) != null;
    }

    @Override
    public void patchSchema(SchemaFactory schemaFactory) {
        if (schemaFactory.getProperty(PERFORM_OCR.getPropertyName()) == null) {
            schemaFactory.newConceptProperty()
                    .concepts(schemaFactory.getOrCreateThingConcept())
                    .name(PERFORM_OCR.getPropertyName())
                    .userVisible(false)
                    .searchable(false)
                    .type(PropertyType.BOOLEAN)
                    .textIndexHints(TextIndexHint.NONE)
                    .save();
        }
    }
}
