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
package io.bigconnect.dw.sentiment.intellidockers;

import com.mware.core.ingest.dataworker.DataWorker;
import com.mware.core.ingest.dataworker.DataWorkerData;
import com.mware.core.ingest.dataworker.DataWorkerPrepareData;
import com.mware.core.ingest.dataworker.ElementOrPropertyStatus;
import com.mware.core.model.Description;
import com.mware.core.model.Name;
import com.mware.core.model.clientapi.dto.VisibilityJson;
import com.mware.core.model.properties.BcSchema;
import com.mware.core.model.properties.RawObjectSchema;
import com.mware.core.model.termMention.TermMentionBuilder;
import com.mware.core.model.termMention.TermMentionRepository;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.ge.Element;
import com.mware.ge.Property;
import com.mware.ge.Vertex;
import com.mware.ge.Visibility;
import com.mware.ge.metric.PausableTimerContext;
import com.mware.ge.metric.Timer;
import com.mware.ge.mutation.ElementMutation;
import com.mware.ge.util.Preconditions;
import com.mware.ge.values.storable.StreamingPropertyValue;
import com.mware.ge.values.storable.Values;
import com.mware.ontology.IgnoredMimeTypes;
import io.bigconnect.dw.text.common.NerUtils;
import io.bigconnect.dw.text.common.Sentiment;
import io.bigconnect.dw.text.common.TextSpan;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Name("IntelliDockers Sentiment Analysis")
@Description("Extracts sentiment from text using IntelliDockers")
public class IntelliDockersSentimentExtractorWorker extends DataWorker {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(IntelliDockersSentimentExtractorWorker.class);
    public static final String CONFIG_INTELLIDOCKERS_URL = "intellidockers.sentiment.url";
    public static final String CONFIG_INTELLIDOCKERS_PARAGRAPHS = "intellidockers.sentiment.paragraphs";

    private Map<String, IntelliDockersSentiment> services = new HashMap<>();
    private boolean doParagraphs;
    private TermMentionRepository termMentionRepository;
    private Timer detectTimer;

    private static final Map<String, String> SUPPORTED_LANGUAGES = new HashMap<String, String>() {{
        put("ro", "ron");
        put("en", "eng");
        put("ar", "ara");
    }};

    @Inject
    public IntelliDockersSentimentExtractorWorker(
            TermMentionRepository termMentionRepository
    ) {
        this.termMentionRepository = termMentionRepository;
    }

    @Override
    public void prepare(DataWorkerPrepareData workerPrepareData) throws Exception {
        super.prepare(workerPrepareData);

        Map<String, Map<String, String>> config = getConfiguration().getMultiValue("intellidockers.sentiment.url");
        Preconditions.checkState(config != null && config.size() > 0, "Please provide the '" + CONFIG_INTELLIDOCKERS_URL+".*" + "' config parameters");

        config.forEach((k,v) -> {
            Retrofit retrofit = new Retrofit.Builder()
                    .baseUrl(v.get(""))
                    .addConverterFactory(JacksonConverterFactory.create())
                    .build();
            services.put(k, retrofit.create(IntelliDockersSentiment.class));
        });

        this.doParagraphs = getConfiguration().getBoolean(CONFIG_INTELLIDOCKERS_PARAGRAPHS, true);
        this.detectTimer = getGraph().getMetricsRegistry().getTimer(getClass(), "sentiment-time");
    }

    @Override
    public boolean isHandled(Element element, Property property) {
        if (property == null) {
            return false;
        }

        if (IgnoredMimeTypes.contains(BcSchema.MIME_TYPE.getFirstPropertyValue(element)))
            return false;

        if (property.getName().equals(RawObjectSchema.RAW_LANGUAGE.getPropertyName())) {
            // do entity extraction only if language is set
            String language = RawObjectSchema.RAW_LANGUAGE.getPropertyValue(property);
            LOGGER.debug("Got language for: "+element.getId()+" - "+language);
            return !StringUtils.isEmpty(language)
                    && SUPPORTED_LANGUAGES.containsKey(language)
                    && services.containsKey(language);
        }

        return false;
    }

    @Override
    public void execute(InputStream in, DataWorkerData data) throws Exception {
        String language = RawObjectSchema.RAW_LANGUAGE.getPropertyValue(data.getProperty());
        Property textProperty = BcSchema.TEXT.getProperty(refresh(data.getElement()), data.getProperty().getKey());
        StreamingPropertyValue spv = BcSchema.TEXT.getPropertyValue(textProperty);

        if (spv == null) {
            LOGGER.warn("Could not find text property for language: "+language);
            return;
        }

        String text = IOUtils.toString(spv.getInputStream(), StandardCharsets.UTF_8);

        ElementMutation<Vertex> m = refresh(data.getElement()).prepareMutation();
        m.deleteProperty(RawObjectSchema.RAW_SENTIMENT.getPropertyName(), Visibility.EMPTY);
        Vertex element = m.save(getAuthorizations());
        getGraph().flush();

        if (StringUtils.isEmpty(text)) {
            getWorkQueueRepository().pushOnDwQueue(
                    element,
                    "",
                    RawObjectSchema.RAW_SENTIMENT.getPropertyName(),
                    data.getWorkspaceId(),
                    data.getVisibilitySource(),
                    data.getPriority(),
                    ElementOrPropertyStatus.DELETION,
                    null);
            pushTextUpdated(data);
            return;
        }

        try {
            LOGGER.info("Extract sentiment for: "+element.getId(), ", language: "+language);
            PausableTimerContext t = new PausableTimerContext(detectTimer);
            Response<SentimentResponse> response = services.get(language).process(new SentimentRequest(text, SUPPORTED_LANGUAGES.get(language)))
                    .execute();
            t.stop();

            if (response.isSuccessful() && response.body() != null) {
                String sentiment = toBcSentiment(response.body());
                LOGGER.debug("Sentiment for: "+element.getId()+" is: "+sentiment);
                m = element.prepareMutation();
                com.mware.ge.Metadata metadata = data.createPropertyMetadata(getUser());
                m.setProperty(RawObjectSchema.RAW_SENTIMENT.getPropertyName(), Values.stringValue(sentiment), metadata, data.getVisibility());
                element = m.save(getAuthorizations());

                getGraph().flush();
            } else {
                LOGGER.info("Could not extract sentiment for: "+element.getId()+": "+response.code()+" - "+response.errorBody());
            }

            if (doParagraphs) {
                NerUtils.removeSentimentTermMentions(element, termMentionRepository, getGraph(), getAuthorizations());
                List<TextSpan> paragraphs = NerUtils.getParagraphs(text);

                VisibilityJson tmVisibilityJson = new VisibilityJson();
                tmVisibilityJson.setSource("");
                for (TextSpan p : paragraphs) {
                    response = services.get(language).process(new SentimentRequest(p.getText(), SUPPORTED_LANGUAGES.get(language)))
                            .execute();
                    SentimentResponse result = response.body();
                    if (result != null) {
                        String sentiment = toBcSentiment(result);
                        TermMentionBuilder tmb = new TermMentionBuilder()
                                .outVertex(element)
                                .propertyKey(textProperty.getKey())
                                .propertyName(textProperty.getName())
                                .start(p.getStart())
                                .end(p.getEnd())
                                .title(String.format("%s: %f", StringUtils.capitalize(sentiment), result.score))
                                .score(result.score)
                                .type("sent")
                                .visibilityJson(tmVisibilityJson)
                                .process(getClass().getName());

                        if ("positive".equals(sentiment)) {
                            tmb.style(String.format("background-color: rgba(0, 255, 0, %f);", result.score / 3));
                        } else {
                            tmb.style(String.format("background-color: rgba(255, 0, 0, %f);", result.score / 3));
                        }

                        tmb.save(getGraph(), getVisibilityTranslator(), getUser(), getAuthorizations());
                    }
                }
                getGraph().flush();
            }
        } catch (IOException e) {
            LOGGER.warn("Could not extract sentiment: %s", e.getMessage());
        }

        getWorkQueueRepository().pushOnDwQueue(
                element,
                "",
                RawObjectSchema.RAW_SENTIMENT.getPropertyName(),
                data.getWorkspaceId(),
                data.getVisibilitySource(),
                data.getPriority(),
                ElementOrPropertyStatus.UPDATE,
                null);

        pushTextUpdated(data);
    }

    private String toBcSentiment(SentimentResponse sentiment) {
        switch (sentiment.label) {
            case "neg":
                return Sentiment.NEGATIVE;
            case "pos":
                return Sentiment.POSITIVE;
            case "neu":
                return Sentiment.NEUTRAL;
            default:
                throw new IllegalArgumentException("Unknown value: "+sentiment.label);
        }
    }
}
