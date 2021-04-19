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
package io.bigconnect.dw.google.speech;

import com.google.cloud.speech.v1.SpeechClient;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.longrunning.OperationsClient;
import com.mware.core.config.Configuration;
import com.mware.core.ingest.dataworker.ElementOrPropertyStatus;
import com.mware.core.lifecycle.LifeSupportService;
import com.mware.core.model.clientapi.dto.VisibilityJson;
import com.mware.core.model.lock.LockRepository;
import com.mware.core.model.properties.BcSchema;
import com.mware.core.model.properties.RawObjectSchema;
import com.mware.core.model.properties.types.PropertyMetadata;
import com.mware.core.model.role.GeAuthorizationRepository;
import com.mware.core.model.workQueue.Priority;
import com.mware.core.model.workQueue.WebQueueRepository;
import com.mware.core.model.workQueue.WorkQueueRepository;
import com.mware.core.user.SystemUser;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.core.util.PeriodicBackgroundService;
import com.mware.ge.*;
import com.mware.ge.query.QueryResultsIterable;
import com.mware.ge.util.Preconditions;
import com.mware.ge.values.storable.DefaultStreamingPropertyValue;
import com.mware.ge.values.storable.TextValue;
import io.bigconnect.dw.google.common.schema.GoogleCredentialUtils;
import io.bigconnect.dw.google.common.schema.GoogleSchemaContribution;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;

import static io.bigconnect.dw.google.speech.Speech2TextSchemaContribution.GOOGLE_S2T_PROGRESS_PROPERTY;

@Singleton
public class Speech2TextOperationMonitorService extends PeriodicBackgroundService {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(Speech2TextOperationMonitorService.class);

    private static final Authorizations AUTHORIZATIONS_ALL = new Authorizations(GeAuthorizationRepository.ADMIN_ROLE);
    private static final int DEFAULT_CHECK_INTERVAL = 10; //seconds
    static final String CONFIG_GOOGLE_S2T_BUCKET_NAME = "google.s2t.bucket.name";

    private WorkQueueRepository workQueueRepository;
    private WebQueueRepository webQueueRepository;
    private final Graph graph;
    private final String bucketName;

    @Inject
    public Speech2TextOperationMonitorService(
            LockRepository lockRepository,
            WorkQueueRepository workQueueRepository,
            WebQueueRepository webQueueRepository,
            Graph graph,
            Configuration configuration,
            LifeSupportService lifeSupportService
    ) {
        super(lockRepository);
        this.workQueueRepository = workQueueRepository;
        this.webQueueRepository = webQueueRepository;
        this.graph = graph;
        this.bucketName = configuration.get(CONFIG_GOOGLE_S2T_BUCKET_NAME, "");

        Preconditions.checkState(!StringUtils.isEmpty(bucketName), "Please provide the " + CONFIG_GOOGLE_S2T_BUCKET_NAME + " configuration property");

        lifeSupportService.add(this);
    }

    @Override
    protected void run() {
        try (QueryResultsIterable<Vertex> pendingVertices = graph.query(AUTHORIZATIONS_ALL)
                .has(GoogleSchemaContribution.OPERATION_NAME.getPropertyName())
                .vertices()) {
            LOGGER.info("Found %s Google responses still pending", pendingVertices.getTotalHits());

            if (pendingVertices.getTotalHits() > 0) {
                try (SpeechClient speechClient = SpeechClient.create()) {
                    final OperationsClient client = speechClient.getOperationsClient();
                    final Storage storage = StorageOptions.newBuilder().setProjectId(GoogleCredentialUtils.getProjectId()).build().getService();

                    pendingVertices.iterator().forEachRemaining(vertex -> {
                        final Property operationNameProp = GoogleSchemaContribution.OPERATION_NAME.getProperty(vertex);

                        if (operationNameProp != null) {
                            final String operationName = operationNameProp.getValue().asObjectCopy().toString();
                            final TextValue language = (TextValue) operationNameProp.getMetadata().getValue("language");
                            LOGGER.debug("Polling operation %s", operationName);
                            if (client.getOperation(operationName).getDone()) {
                                LOGGER.debug("Google operation %s finished", operationName);

                                final String resultedText = client.getOperation(operationName).getResponse().getValue().toStringUtf8();

                                PropertyMetadata propertyMetadata = new PropertyMetadata(
                                        new SystemUser(), new VisibilityJson(), Visibility.EMPTY
                                );
                                propertyMetadata.add(BcSchema.TEXT_LANGUAGE_METADATA.getMetadataKey(), language, Visibility.EMPTY);
                                BcSchema.TEXT.addPropertyValue(
                                        vertex,
                                        language.stringValue(),
                                        DefaultStreamingPropertyValue.create(resultedText),
                                        propertyMetadata.createMetadata(), Visibility.EMPTY, vertex.getAuthorizations()
                                );

                                // add also the new language
                                RawObjectSchema.RAW_LANGUAGE.addPropertyValue(vertex, language.stringValue(), language.stringValue(),
                                        null, Visibility.EMPTY, vertex.getAuthorizations());

                                webQueueRepository.pushTextUpdated(vertex.getId(), Priority.HIGH);

                                GOOGLE_S2T_PROGRESS_PROPERTY.setProperty(vertex, Boolean.FALSE, Visibility.EMPTY, vertex.getAuthorizations());

                                // Cleanup
                                GoogleSchemaContribution.OPERATION_NAME.removeProperty(vertex, AUTHORIZATIONS_ALL);
                                graph.flush();

                                workQueueRepository.pushGraphPropertyQueue(
                                        vertex,
                                        language.stringValue(),
                                        RawObjectSchema.RAW_LANGUAGE.getPropertyName(),
                                        null,
                                        null,
                                        Priority.HIGH,
                                        ElementOrPropertyStatus.UPDATE,
                                        null
                                );

                                final String meta = client.getOperation(operationName).getMetadata().getValue().toStringUtf8();
                                if (!StringUtils.isEmpty(meta)) {
                                    String[] gcsUrl = meta.split("/");
                                    if (gcsUrl.length > 0) {
                                        storage.delete(bucketName, gcsUrl[gcsUrl.length - 1]);
                                    }
                                }
                            }
                        }
                    });
                } catch (IOException e) {
                    LOGGER.error("There was an error while polling Google responses with message: %s", e.getMessage());
                    e.printStackTrace();
                }
            }
        } catch (IOException ex) {
            LOGGER.error("There was an error while closing elastic iterator with message: %s", ex.getMessage());
            ex.printStackTrace();
        }
    }

    @Override
    protected int getCheckIntervalSeconds() {
        return DEFAULT_CHECK_INTERVAL;
    }
}
