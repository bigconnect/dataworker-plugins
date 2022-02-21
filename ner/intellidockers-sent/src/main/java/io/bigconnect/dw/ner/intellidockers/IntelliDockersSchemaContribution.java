/*
 * This file is part of the BigConnect project.
 *
 * Copyright (c) 2013-2020 MWARE SOLUTIONS SRL
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License version 3
 * as published by the Free Software Foundation with the addition of the
 * following permission added to Section 15 as permitted in Section 7(a):
 * FOR ANY PART OF THE COVERED WORK IN WHICH THE COPYRIGHT IS OWNED BY
 * MWARE SOLUTIONS SRL, MWARE SOLUTIONS SRL DISCLAIMS THE WARRANTY OF
 * NON INFRINGEMENT OF THIRD PARTY RIGHTS

 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License
 * along with this program; if not, see http://www.gnu.org/licenses or write to
 * the Free Software Foundation, Inc., 51 Franklin Street, Fifth Floor,
 * Boston, MA, 02110-1301 USA, or download the license from the following URL:
 * https://www.gnu.org/licenses/agpl-3.0.txt
 *
 * The interactive user interfaces in modified source and object code versions
 * of this program must display Appropriate Legal Notices, as required under
 * Section 5 of the GNU Affero General Public License.
 *
 * You can be released from the requirements of the license by purchasing
 * a commercial license. Buying such a license is mandatory as soon as you
 * develop commercial activities involving the BigConnect software without
 * disclosing the source code of your own applications.
 *
 * These activities include: offering paid services to customers as an ASP,
 * embedding the product in a web application, shipping BigConnect with a
 * closed source product.
 */
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
