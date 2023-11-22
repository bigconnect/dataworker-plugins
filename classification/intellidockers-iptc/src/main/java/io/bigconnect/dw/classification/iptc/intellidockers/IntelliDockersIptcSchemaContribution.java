package io.bigconnect.dw.classification.iptc.intellidockers;

import com.mware.core.model.clientapi.dto.PropertyType;
import com.mware.core.model.properties.types.DoubleMetadataBcProperty;
import com.mware.core.model.properties.types.StringBcProperty;
import com.mware.core.model.properties.types.StringMetadataBcProperty;
import com.mware.core.model.schema.SchemaContribution;
import com.mware.core.model.schema.SchemaFactory;
import com.mware.ge.TextIndexHint;

public class IntelliDockersIptcSchemaContribution implements SchemaContribution {
    public static final StringBcProperty IPTC = new StringBcProperty("iptc");
    public static final DoubleMetadataBcProperty IPTC_SCORE = new DoubleMetadataBcProperty("iptc_score");

    @Override
    public boolean patchApplied(SchemaFactory schemaFactory) {
        return schemaFactory.getProperty(IPTC.getPropertyName()) != null;
    }

    @Override
    public void patchSchema(SchemaFactory schemaFactory) {
        if (schemaFactory.getProperty(IPTC.getPropertyName()) == null) {
            schemaFactory.newConceptProperty()
                    .concepts(schemaFactory.getOrCreateThingConcept())
                    .displayName("IPTC")
                    .name(IPTC.getPropertyName())
                    .userVisible(true)
                    .searchable(true)
                    .type(PropertyType.STRING)
                    .textIndexHints(TextIndexHint.EXACT_MATCH)
                    .save();
        }
    }
}
