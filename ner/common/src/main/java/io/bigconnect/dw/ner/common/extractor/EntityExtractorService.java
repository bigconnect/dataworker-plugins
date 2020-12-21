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
package io.bigconnect.dw.ner.common.extractor;

import com.mware.core.config.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;

public class EntityExtractorService {

    private static final Logger logger = LoggerFactory.getLogger(EntityExtractorService.class);
    private static EntityExtractorService service;
    private ServiceLoader<EntityExtractor> loader;
    private Configuration configuration;

    private EntityExtractorService(Configuration configuration) {
        this.configuration = configuration;
        loader = ServiceLoader.load(EntityExtractor.class);
    }

    public static synchronized EntityExtractorService getInstance(Configuration configuration) {
        if (service == null) {
            service = new EntityExtractorService(configuration);
        }
        return service;
    }

    public void initialize(Configuration config) throws Exception {
        Iterator<EntityExtractor> extractors = loader.iterator();
        logger.info("Initializing NER Extractors");
        while (extractors.hasNext()) {
            EntityExtractor currentExtractor = extractors.next();
            logger.info("Initializing Extractor - {}", currentExtractor.getName());
            currentExtractor.initialize(config);
        }

    }
    
    public ExtractedEntities extractEntities(String languageCode, String textToParse, boolean manuallyReplaceDemonyms) {
        ExtractedEntities e = new ExtractedEntities(configuration);
        try {
            Iterator<EntityExtractor> extractors = loader.iterator();
            while (extractors != null && extractors.hasNext()) {
                EntityExtractor currentExtractor = extractors.next();
                ExtractedEntities e2 = currentExtractor.extractEntities(languageCode, textToParse, manuallyReplaceDemonyms);
                e.merge(e2);
            }
        } catch (ServiceConfigurationError serviceError) {
            e = null;
            serviceError.printStackTrace();
        }
        return e;
    }

    @SuppressWarnings("rawtypes")
    public ExtractedEntities extractEntitiesFromSentences(String languageCode, Map[] sentences, boolean manuallyReplaceDemonyms) {
        ExtractedEntities e = new ExtractedEntities(configuration);
        try {
            Iterator<EntityExtractor> extractors = loader.iterator();
            while (extractors != null && extractors.hasNext()) {
                EntityExtractor currentExtractor = extractors.next();
                ExtractedEntities e2 = currentExtractor.extractEntitiesFromSentences(languageCode, sentences, manuallyReplaceDemonyms);
                e.merge(e2);
            }
        } catch (ServiceConfigurationError serviceError) {
            e = null;
            serviceError.printStackTrace();
        }
        return e;
    }

}
