package io.bigconnect.dw.tester;

import com.mware.core.model.clientapi.dto.VisibilityJson;
import com.mware.core.model.properties.BcSchema;
import com.mware.core.model.properties.types.PropertyMetadata;
import com.mware.core.model.schema.SchemaConstants;
import com.mware.core.model.workQueue.Priority;
import com.mware.core.model.workQueue.WorkQueueRepository;
import com.mware.core.user.SystemUser;
import com.mware.ge.*;
import com.mware.ge.mutation.ElementMutation;
import com.mware.ge.mutation.ExistingElementMutation;
import com.mware.ge.values.storable.ByteArray;
import com.mware.ge.values.storable.DefaultStreamingPropertyValue;
import com.mware.ge.values.storable.StreamingPropertyValue;
import com.mware.ge.values.storable.Values;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.ZonedDateTime;

public class EntityCreator {
    private Graph graph;
    private WorkQueueRepository workQueueRepository;
    private Authorizations authorizations;
    private Element element;

    public static EntityCreator build(Graph graph, WorkQueueRepository workQueueRepository) {
        return new EntityCreator(graph, workQueueRepository);
    }

    private EntityCreator(Graph graph, WorkQueueRepository workQueueRepository) {
        this.graph = graph;
        this.workQueueRepository = workQueueRepository;
        this.authorizations = new Authorizations();
    }

    public EntityCreator with(Element element) {
        this.element = element;
        return this;
    }

    public EntityCreator newVideo(String title, InputStream file) {
        element = graph.prepareVertex(Visibility.EMPTY, SchemaConstants.CONCEPT_TYPE_VIDEO)
                .save(authorizations);

        setTitle(title);
        try {
            addRaw(IOUtils.toByteArray(file));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return this;
    }
    
    public EntityCreator newDocument(String title, String text) {
        element = graph.prepareVertex(Visibility.EMPTY, SchemaConstants.CONCEPT_TYPE_DOCUMENT)
                .save(authorizations);

        setTitle(title);
        setText("", text);
        return this;
    }

    public EntityCreator setText(String key, String text) {
        Metadata propertyMetadata = createPropertyMetadata();
        BcSchema.MIME_TYPE_METADATA.setMetadata(propertyMetadata, "text/plain", Visibility.EMPTY);
        BcSchema.TEXT_DESCRIPTION_METADATA.setMetadata(propertyMetadata, "Text", Visibility.EMPTY);

        String value = StringUtils.trimToEmpty(text);

        element = element.prepareMutation()
                .addPropertyValue(key, BcSchema.TEXT.getPropertyName(), DefaultStreamingPropertyValue.create(value), propertyMetadata, Visibility.EMPTY)
                .save(authorizations);

        return this;
    }

    public EntityCreator addRaw(byte[] bytes) {
        StreamingPropertyValue rawValue = DefaultStreamingPropertyValue.create(new ByteArrayInputStream(bytes), ByteArray.class);
        rawValue.searchIndex(false);
        element = element.prepareMutation()
                .setProperty(BcSchema.RAW.getPropertyName(), rawValue, createPropertyMetadata(), Visibility.EMPTY)
                .save(authorizations);
        return this;
    }

    public EntityCreator setTitle(String title) {
        element = element.prepareMutation()
                .setProperty(BcSchema.TITLE.getPropertyName(), Values.stringValue(title), createPropertyMetadata(), Visibility.EMPTY)
                .save(authorizations);
        return this;
    }

    public EntityCreator setProperty(String property, Object value) {
        element = element.prepareMutation()
                .setProperty(property, Values.of(value), createPropertyMetadata(), Visibility.EMPTY)
                .save(authorizations);

        return this;
    }

    public Metadata createPropertyMetadata() {
        return new PropertyMetadata(
                ZonedDateTime.now(),
                new SystemUser(),
                new VisibilityJson(),
                Visibility.EMPTY
        ).createMetadata();
    }

    public Element push() {
        return push(null, null);
    }

    public Element push(String property, String key) {
        workQueueRepository.pushOnDwQueue(
                element,
                key,
                property,
                null,
                null,
                Priority.HIGH,
                null,
                null
        );
        return element;
    }
}
