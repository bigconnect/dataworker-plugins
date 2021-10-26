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
package io.bigconnect.dw.ner.common.util;

import com.google.gson.Gson;
import com.mware.core.config.Configuration;
import com.mware.ge.metric.GeMetricRegistry;
import io.bigconnect.dw.ner.common.extractor.ExtractedEntities;
import io.bigconnect.dw.ner.common.extractor.OrganizationOccurrence;
import io.bigconnect.dw.ner.common.extractor.PersonOccurrence;
import io.bigconnect.dw.ner.common.extractor.SentenceLocationOccurrence;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

@SuppressWarnings({"rawtypes", "unchecked"})
public class MuckUtils {

    public static ExtractedEntities entitiesFromNlpJsonString(String nlpJsonString, Configuration configuration, GeMetricRegistry metricRegistry) {
        Map sentences = sentencesFromJsonString(nlpJsonString);
        return entitiesFromNlpSentenceMap(sentences, configuration, metricRegistry);
    }

    public static Map sentencesFromJsonString(String nlpJsonString) {
        Gson gson = new Gson();
        Map content = gson.fromJson(nlpJsonString, Map.class);
        return content;
    }

    private static ExtractedEntities entitiesFromNlpSentenceMap(Map mcSentences, Configuration configuration, GeMetricRegistry metricRegistry) {
        ExtractedEntities entities = new ExtractedEntities(configuration, metricRegistry);
        Iterator it = mcSentences.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pairs = (Map.Entry) it.next();
            String storySentencesId = pairs.getKey().toString();
            if (storySentencesId.equals('_')) {
                continue;
            }
            Map corenlp = (Map) pairs.getValue();
            List<Map> nlpSentences = (List<Map>) ((Map) corenlp.get("corenlp")).get("sentences");
            for (Map sentence : nlpSentences) { // one mc sentence could be multiple corenlp sentences
                String queuedEntityText = null;
                String lastEntityType = null;
                List<Map> tokens = (List<Map>) sentence.get("tokens");
                for (Map token : tokens) {
                    String entityType = (String) token.get("ne");
                    String tokenText = (String) token.get("word");
                    if (entityType.equals(lastEntityType)) {
                        queuedEntityText += " " + tokenText;
                    } else {
                        if (queuedEntityText != null && lastEntityType != null) {
                            //TODO: figure out if we need the character index here or not
                            switch (lastEntityType) {
                                case "PERSON":
                                    entities.addPerson(new PersonOccurrence(queuedEntityText, 0));
                                    break;
                                case "LOCATION":
                                    entities.addLocation(new SentenceLocationOccurrence(queuedEntityText, storySentencesId));
                                    break;
                                case "ORGANIZATION":
                                    entities.addOrganization(new OrganizationOccurrence(queuedEntityText, 0));
                                    break;
                            }
                        }
                        queuedEntityText = tokenText;
                    }
                    lastEntityType = entityType;
                }
            }
            it.remove(); // avoids a ConcurrentModificationException
        }
        return entities;
    }

}
