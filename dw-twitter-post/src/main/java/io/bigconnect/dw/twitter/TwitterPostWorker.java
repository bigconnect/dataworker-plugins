package io.bigconnect.dw.twitter;

import com.mware.core.ingest.dataworker.DataWorker;
import com.mware.core.ingest.dataworker.DataWorkerData;
import com.mware.core.model.Description;
import com.mware.core.model.Name;
import com.mware.core.model.properties.BcSchema;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.ge.Element;
import com.mware.ge.Property;
import com.mware.ge.Vertex;
import com.mware.ge.Visibility;
import com.mware.ge.mutation.ElementMutation;
import com.mware.ge.values.storable.StreamingPropertyValue;
import com.mware.ge.values.storable.Values;
import org.apache.commons.lang3.StringUtils;

import java.io.InputStream;

@Name("Twitter Post")
@Description("Add text from retweet when title is blank")
public class TwitterPostWorker extends DataWorker {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(TwitterPostWorker.class);

    @Override
    public boolean isHandled(Element element, Property property) {
        LOGGER.debug("TwitterPostWorker - isHandled() start");

        String conceptType = ((Vertex) element).getConceptType();
        if ("twitterPost".equalsIgnoreCase(conceptType)) {
            String title = BcSchema.TITLE.getFirstPropertyValue((Vertex) element);

            Property textProp = element.getProperty("text_from_retweet");

            if (StringUtils.isBlank(title) && textProp != null && textProp.getValue() != null) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void execute(InputStream in, DataWorkerData data) throws Exception {
        Element element = data.getElement();

        Property textFromRetweetProp = element.getProperty("text_from_retweet");

        if (textFromRetweetProp != null && textFromRetweetProp.getValue() != null) {
            Object raw = textFromRetweetProp.getValue().asObject();

            if (raw instanceof String) {
                String textFromRetweet = (String) raw;
                if (StringUtils.isNotBlank(textFromRetweet)) {
                    LOGGER.debug("TwitterPostWorker - execute() textFromRetweet");
                    ElementMutation<Element> mutation = element.prepareMutation();
                    mutation.setProperty(BcSchema.TITLE.getPropertyName(), Values.stringValue(textFromRetweet), Visibility.EMPTY);
                    StreamingPropertyValue streamingPropertyValue = StreamingPropertyValue.create(textFromRetweet);

                    mutation.setProperty(BcSchema.TEXT.getPropertyName(), streamingPropertyValue, Visibility.EMPTY);
                    mutation.save(getAuthorizations());
                    getGraph().flush();
                }
            }
        }
    }
}
