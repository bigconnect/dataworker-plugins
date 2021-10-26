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
package io.bigconnect.dw.image.metadata;

import com.google.inject.Inject;
import com.mware.bigconnect.image.ImageTransform;
import com.mware.bigconnect.image.ImageTransformExtractor;
import com.mware.core.ingest.dataworker.DataWorkerData;
import com.mware.core.ingest.dataworker.DataWorkerPrepareData;
import com.mware.core.ingest.dataworker.PostMimeTypeWorker;
import com.mware.core.model.Description;
import com.mware.core.model.Name;
import com.mware.core.model.properties.types.BcPropertyUpdate;
import com.mware.core.model.properties.types.BooleanSingleValueBcProperty;
import com.mware.core.model.properties.types.IntegerSingleValueBcProperty;
import com.mware.core.model.schema.SchemaRepository;
import com.mware.ge.Authorizations;
import com.mware.ge.Element;
import com.mware.ge.Metadata;
import com.mware.ge.Vertex;
import com.mware.ge.mutation.ExistingElementMutation;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

@Name("Drewnoakes Image Metadata")
@Description("Extracts image metadata using Drewnoakes after MIME type")
public class ImageOrientationPostMimeTypeWorker extends PostMimeTypeWorker {
    private SchemaRepository ontologyRepository;
    private BooleanSingleValueBcProperty yAxisFlippedProperty;
    private IntegerSingleValueBcProperty clockwiseRotationProperty;

    @Override
    public void prepare(DataWorkerPrepareData workerPrepareData) throws Exception {
        super.prepare(workerPrepareData);
        yAxisFlippedProperty = new BooleanSingleValueBcProperty(ontologyRepository.getRequiredPropertyNameByIntent("media.yAxisFlipped"));
        clockwiseRotationProperty = new IntegerSingleValueBcProperty(ontologyRepository.getRequiredPropertyNameByIntent("media.clockwiseRotation"));
    }

    @Override
    public void execute(String mimeType, DataWorkerData data, Authorizations authorizations) throws Exception {
        if (!mimeType.startsWith("image")) {
            return;
        }

        File localFile = getLocalFileForRaw(data.getElement());
        Metadata metadata = data.createPropertyMetadata(getUser());
        ExistingElementMutation<Vertex> mutation = refresh(data.getElement(), authorizations).prepareMutation();

        ImageTransform imageTransform = ImageTransformExtractor.getImageTransform(localFile);
        List<BcPropertyUpdate> changedProperties = new ArrayList<>();
        yAxisFlippedProperty.updateProperty(changedProperties, data.getElement(), mutation, imageTransform.isYAxisFlipNeeded(), metadata, data.getVisibility());
        clockwiseRotationProperty.updateProperty(changedProperties, data.getElement(), mutation, imageTransform.getCWRotationNeeded(), metadata, data.getVisibility());

        Element e = mutation.save(authorizations);
        getGraph().flush();

        getWebQueueRepository().broadcastPropertiesChange(e, changedProperties, null, data.getPriority());
        getWorkQueueRepository().pushOnDwQueue(
                e,
                changedProperties,
                null,
                null,
                data.getPriority()
        );
    }

    @Inject
    public void setSchemaRepository(SchemaRepository schemaRepository) {
        this.ontologyRepository = schemaRepository;
    }
}
