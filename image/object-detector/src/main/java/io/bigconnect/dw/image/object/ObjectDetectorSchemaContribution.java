package io.bigconnect.dw.image.object;

import com.mware.core.model.clientapi.dto.PropertyType;
import com.mware.core.model.properties.types.BooleanSingleValueBcProperty;
import com.mware.core.model.properties.types.StringSingleValueBcProperty;
import com.mware.core.model.schema.SchemaContribution;
import com.mware.core.model.schema.SchemaFactory;
import com.mware.ge.TextIndexHint;

public class ObjectDetectorSchemaContribution implements SchemaContribution {
    public static final BooleanSingleValueBcProperty DETECT_OBJECTS = new BooleanSingleValueBcProperty("detectObjects");

    @Override
    public boolean patchApplied(SchemaFactory schemaFactory) {
        return schemaFactory.getProperty(DETECT_OBJECTS.getPropertyName()) != null;
    }

    @Override
    public void patchSchema(SchemaFactory schemaFactory) {
        if (schemaFactory.getProperty(DETECT_OBJECTS.getPropertyName()) == null) {
            schemaFactory.newConceptProperty()
                    .concepts(schemaFactory.getOrCreateThingConcept())
                    .name(DETECT_OBJECTS.getPropertyName())
                    .userVisible(false)
                    .searchable(false)
                    .type(PropertyType.BOOLEAN)
                    .textIndexHints(TextIndexHint.NONE)
                    .save();
        }
    }
}
