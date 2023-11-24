package io.bigconnect.dw.image.face;

import com.mware.core.model.clientapi.dto.PropertyType;
import com.mware.core.model.properties.types.BooleanSingleValueBcProperty;
import com.mware.core.model.schema.SchemaContribution;
import com.mware.core.model.schema.SchemaFactory;
import com.mware.ge.TextIndexHint;

public class FaceDetectorSchemaContribution implements SchemaContribution {
    public static final BooleanSingleValueBcProperty DETECT_FACES = new BooleanSingleValueBcProperty("detectFaces");

    @Override
    public boolean patchApplied(SchemaFactory schemaFactory) {
        return schemaFactory.getProperty(DETECT_FACES.getPropertyName()) != null;
    }

    @Override
    public void patchSchema(SchemaFactory schemaFactory) {
        if (schemaFactory.getProperty(DETECT_FACES.getPropertyName()) == null) {
            schemaFactory.newConceptProperty()
                    .concepts(schemaFactory.getOrCreateThingConcept())
                    .name(DETECT_FACES.getPropertyName())
                    .userVisible(false)
                    .searchable(false)
                    .type(PropertyType.BOOLEAN)
                    .textIndexHints(TextIndexHint.NONE)
                    .save();
        }
    }
}
