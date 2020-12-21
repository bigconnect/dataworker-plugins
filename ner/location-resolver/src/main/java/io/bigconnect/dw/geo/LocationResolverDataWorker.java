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
package io.bigconnect.dw.geo;

import com.bericotech.clavin.extractor.LocationOccurrence;
import com.bericotech.clavin.resolver.ResolvedLocation;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.mware.core.ingest.dataworker.DataWorker;
import com.mware.core.ingest.dataworker.DataWorkerData;
import com.mware.core.ingest.dataworker.DataWorkerPrepareData;
import com.mware.core.model.properties.BcSchema;
import com.mware.core.model.properties.RawObjectSchema;
import com.mware.core.model.schema.SchemaConstants;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.ontology.IgnoredMimeTypes;
import io.bigconnect.dw.ner.common.ParseManager;
import io.bigconnect.dw.ner.common.places.CliffLocationResolver;
import com.mware.ge.Element;
import com.mware.ge.Property;
import com.mware.ge.Vertex;
import com.mware.ge.type.GeoPoint;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.InputStream;
import java.util.List;

public class LocationResolverDataWorker extends DataWorker {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(LocationResolverDataWorker.class);
    private CliffLocationResolver resolver;

    @Inject
    public LocationResolverDataWorker() {
    }

    @Override
    public void prepare(DataWorkerPrepareData workerPrepareData) throws Exception {
        super.prepare(workerPrepareData);

        File gazetteerDir = new File(getConfiguration().get(ParseManager.GEOINDEX_PATH, ""));
        if (gazetteerDir.exists() && gazetteerDir.isDirectory()) {
            resolver = ParseManager.getLocationResolver(getConfiguration());
        } else {
            LOGGER.error("Missing gazetter! Download and build a CLAVIN IndexDirectory at " + getConfiguration().get(ParseManager.GEOINDEX_PATH, ""));
        }
    }

    @Override
    public void execute(InputStream in, DataWorkerData data) throws Exception {
        String title = BcSchema.TITLE.getFirstPropertyValue(data.getElement());
        if (StringUtils.isEmpty(title))
            return;

        List<LocationOccurrence> locationOccurrences = Lists.newArrayList(new LocationOccurrence(title, 0));
        List<ResolvedLocation> resolvedLocations = resolver.resolveLocations(locationOccurrences, CliffLocationResolver.MAX_HIT_DEPTH, -1, false);
        if (resolvedLocations != null && resolvedLocations.size() > 0) {
            ResolvedLocation resolvedLocation = resolvedLocations.get(0);
            GeoPoint geoPoint = new GeoPoint(resolvedLocation.getGeoname().getLatitude(), resolvedLocation.getGeoname().getLongitude());
            RawObjectSchema.GEOLOCATION_PROPERTY.addPropertyValue(data.getElement(), "", geoPoint, data.getElement().getVisibility(), getAuthorizations());
            getGraph().flush();
        }
    }

    @Override
    public boolean isHandled(Element element, Property property) {
        if (resolver == null)
            return false;

        if (element == null && property != null)
            return false;

        if (!isVertex(element))
            return false;

        return SchemaConstants.CONCEPT_TYPE_LOCATION.equals(((Vertex)element).getConceptType())
                && !IgnoredMimeTypes.contains(BcSchema.MIME_TYPE.getFirstPropertyValue(element));
    }
}
