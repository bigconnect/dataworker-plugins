package io.bigconnect.dw.image.face;

import com.mware.core.model.clientapi.dto.PropertyType;
import com.mware.core.model.properties.types.BooleanSingleValueBcProperty;
import com.mware.core.model.properties.types.IntegerSingleValueBcProperty;
import com.mware.core.model.properties.types.StringSingleValueBcProperty;
import com.mware.core.model.schema.SchemaContribution;
import com.mware.core.model.schema.SchemaFactory;
import com.mware.ge.TextIndexHint;

public class FaceDetectorSchemaContribution implements SchemaContribution {
    public static final BooleanSingleValueBcProperty DETECT_FACES = new BooleanSingleValueBcProperty("detectFaces");
    public static final IntegerSingleValueBcProperty PERSON_AGE = new IntegerSingleValueBcProperty("personAge");
    public static final StringSingleValueBcProperty PERSON_SEX = new StringSingleValueBcProperty("personSex");

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

        if (schemaFactory.getProperty(PERSON_AGE.getPropertyName()) == null) {
            schemaFactory.newConceptProperty()
                    .concepts(schemaFactory.getOrCreateThingConcept())
                    .name(PERSON_AGE.getPropertyName())
                    .displayName("Varsta")
                    .userVisible(true)
                    .searchable(true)
                    .type(PropertyType.INTEGER)
                    .textIndexHints(TextIndexHint.NONE)
                    .save();
        }

        if (schemaFactory.getProperty(PERSON_SEX.getPropertyName()) == null) {
            schemaFactory.newConceptProperty()
                    .concepts(schemaFactory.getOrCreateThingConcept())
                    .name(PERSON_AGE.getPropertyName())
                    .displayName("Sex")
                    .userVisible(true)
                    .searchable(true)
                    .type(PropertyType.STRING)
                    .textIndexHints(TextIndexHint.EXACT_MATCH)
                    .save();
        }
    }
}
