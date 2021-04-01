package io.bigconnect.dw.ner.intellidockers;

import com.mware.core.model.properties.SchemaProperties;
import com.mware.core.model.schema.Schema;
import com.mware.core.model.schema.SchemaContribution;
import com.mware.core.model.schema.SchemaFactory;
import com.mware.core.model.schema.SchemaRepository;
import com.mware.ge.values.storable.BooleanValue;

import java.util.Arrays;
import java.util.Set;

public class IntelliDockersSchemaContribution implements SchemaContribution {
    protected static final String CONCEPT_TYPE_PHONE_NUMBER = "phoneNumber";
    protected static final String CONCEPT_TYPE_CREDIT_CARD = "creditCard";
    protected static final String CONCEPT_TYPE_EMAIL = "email";
    protected static final String CONCEPT_TYPE_NATIONALITY = "nationality";
    protected static final String CONCEPT_TYPE_RELIGION = "religion";
    protected static final String CONCEPT_TYPE_URL = "url";
    protected static final String CONCEPT_TYPE_PERSONAL_ID = "personalId";

    @Override
    public boolean patchApplied(SchemaFactory schemaFactory) {
        Schema schema = schemaFactory.getSchemaRepository().getOntology(SchemaRepository.PUBLIC);
        Set<String> concepts = schema.getConceptsByName().keySet();
        return concepts.containsAll(Arrays.asList(
                CONCEPT_TYPE_PHONE_NUMBER, CONCEPT_TYPE_CREDIT_CARD, CONCEPT_TYPE_EMAIL,
                CONCEPT_TYPE_NATIONALITY, CONCEPT_TYPE_RELIGION, CONCEPT_TYPE_URL,
                CONCEPT_TYPE_PERSONAL_ID
        ));
    }

    @Override
    public void patchSchema(SchemaFactory schemaFactory) {
        schemaFactory.newConcept()
                .conceptType(CONCEPT_TYPE_PHONE_NUMBER)
                .displayName("Phone Number")
                .glyphIcon(getClass().getResourceAsStream("/phone_number.png"))
                .property(SchemaProperties.USER_VISIBLE.getPropertyName(), BooleanValue.TRUE)
                .save();

        schemaFactory.newConcept()
                .conceptType(CONCEPT_TYPE_CREDIT_CARD)
                .displayName("Credit Card")
                .glyphIcon(getClass().getResourceAsStream("/credit_card.png"))
                .property(SchemaProperties.USER_VISIBLE.getPropertyName(), BooleanValue.TRUE)
                .save();

        schemaFactory.newConcept()
                .conceptType(CONCEPT_TYPE_EMAIL)
                .displayName("Email")
                .glyphIcon(getClass().getResourceAsStream("/email.png"))
                .property(SchemaProperties.USER_VISIBLE.getPropertyName(), BooleanValue.TRUE)
                .save();

        schemaFactory.newConcept()
                .conceptType(CONCEPT_TYPE_NATIONALITY)
                .displayName("Nationality")
                .glyphIcon(getClass().getResourceAsStream("/nationality.png"))
                .property(SchemaProperties.USER_VISIBLE.getPropertyName(), BooleanValue.TRUE)
                .save();

        schemaFactory.newConcept()
                .conceptType(CONCEPT_TYPE_RELIGION)
                .displayName("Religion")
                .glyphIcon(getClass().getResourceAsStream("/religion.png"))
                .property(SchemaProperties.USER_VISIBLE.getPropertyName(), BooleanValue.TRUE)
                .save();

        schemaFactory.newConcept()
                .conceptType(CONCEPT_TYPE_URL)
                .displayName("URL")
                .glyphIcon(getClass().getResourceAsStream("/url.png"))
                .property(SchemaProperties.USER_VISIBLE.getPropertyName(), BooleanValue.TRUE)
                .save();

        schemaFactory.newConcept()
                .conceptType(CONCEPT_TYPE_PERSONAL_ID)
                .displayName("Personal ID")
                .glyphIcon(getClass().getResourceAsStream("/id_card.png"))
                .property(SchemaProperties.USER_VISIBLE.getPropertyName(), BooleanValue.TRUE)
                .save();
    }
}
