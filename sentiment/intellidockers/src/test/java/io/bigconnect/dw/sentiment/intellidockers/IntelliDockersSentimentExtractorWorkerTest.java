package io.bigconnect.dw.sentiment.intellidockers;

import com.mware.core.InMemoryGraphTestBase;
import com.mware.core.ingest.dataworker.DataWorkerData;
import com.mware.core.ingest.dataworker.DataWorkerPrepareData;
import com.mware.core.model.properties.BcSchema;
import com.mware.core.model.properties.RawObjectSchema;
import com.mware.core.model.schema.SchemaConstants;
import com.mware.core.model.termMention.TermMentionRepository;
import com.mware.core.model.workQueue.Priority;
import com.mware.core.security.DirectVisibilityTranslator;
import com.mware.ge.Authorizations;
import com.mware.ge.Vertex;
import com.mware.ge.Visibility;
import com.mware.ge.values.storable.ByteArray;
import com.mware.ge.values.storable.DefaultStreamingPropertyValue;
import com.mware.ge.values.storable.Values;
import io.bigconnect.dw.text.common.Sentiment;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

import static io.bigconnect.dw.sentiment.intellidockers.IntelliDockersSentimentExtractorWorker.CONFIG_INTELLIDOCKERS_PARAGRAPHS;
import static io.bigconnect.dw.sentiment.intellidockers.IntelliDockersSentimentExtractorWorker.CONFIG_INTELLIDOCKERS_URL;

public class IntelliDockersSentimentExtractorWorkerTest extends InMemoryGraphTestBase {
    Authorizations AUTHS = new Authorizations();
    String TEXT_RO = "Directorul Termoenergetica, Claudiu Crețu, spune că ar plăti toate datoriile dacă ar avea bani: ”Când plătește Primăria subvenția, plătim și noi”.\n" +
            "\n" +
            "Primăria Capitalei are o datorie de peste 200 de milioane lei la Termoenergetica, reprezentând plata subvenției asumate pentru populație pentru ultimele luni, mai-septembrie.\n" +
            "\n" +
            "În București, doar 10-12% din rețeaua principală de termoficare este modernizată, iar pierderile sunt colosale, undeva la 2.200.000 l/h în 2020.\n" +
            "\n" +
            "Potrivit informațiilor făcute publice de compania Termoenergetica, în cei 4 ani de mandat ai Gabrielei Firea s-au modernizat peste 200 de km de rețea de termoficare, din care doar puțin peste 30 km de rețea primară.\n" +
            "\n" +
            "Într-un interviu acordat HotNews.ro la finalul lunii septembrie, Nicușor Dan declara că în 2021 au fost modernizați 15 km de rețea primară.";


    @Test
    public void testSentimentParagraphs() throws Exception {
        getConfiguration().set(CONFIG_INTELLIDOCKERS_URL+".ro", "http://localhost:28001");
        getConfiguration().set(CONFIG_INTELLIDOCKERS_URL+".en", "http://localhost:28002");
        getConfiguration().set(CONFIG_INTELLIDOCKERS_URL+".ar", "http://localhost:28003");
        getConfiguration().set(CONFIG_INTELLIDOCKERS_PARAGRAPHS, "true");

        IntelliDockersSentimentExtractorWorker dw = new IntelliDockersSentimentExtractorWorker(getTermMentionRepository());
        dw.setConfiguration(getConfiguration());
        dw.setGraph(getGraph());
        dw.setWorkQueueRepository(getWorkQueueRepository());
        dw.setWebQueueRepository(getWebQueueRepository());
        dw.setVisibilityTranslator(new DirectVisibilityTranslator());

        dw.prepare(new DataWorkerPrepareData(getConfigurationMap(), Collections.emptyList(), getUser(), AUTHS, null));

        Vertex v = getGraph().prepareVertex(Visibility.EMPTY, SchemaConstants.CONCEPT_TYPE_DOCUMENT)
                .setProperty(RawObjectSchema.RAW_LANGUAGE.getPropertyName(), Values.stringValue("ro"), Visibility.EMPTY)
                .setProperty(BcSchema.TEXT.getPropertyName(), new DefaultStreamingPropertyValue(new ByteArrayInputStream(TEXT_RO.getBytes(StandardCharsets.UTF_8)), ByteArray.class), Visibility.EMPTY)
                .save(AUTHS);

        DataWorkerData data = new DataWorkerData(new DirectVisibilityTranslator(), v, RawObjectSchema.RAW_LANGUAGE.getFirstProperty(v),
                null, null, Priority.NORMAL, true);
        if (dw.isHandled(v, RawObjectSchema.RAW_LANGUAGE.getFirstProperty(v))) {
            dw.execute(new ByteArrayInputStream(TEXT_RO.getBytes(StandardCharsets.UTF_8)), data);
        }

        v = getGraph().getVertex(v.getId(), AUTHS);

        Assert.assertEquals(Sentiment.NEGATIVE, RawObjectSchema.RAW_SENTIMENT.getPropertyValue(v));

        Authorizations tmAuths = getGraph().createAuthorizations(TermMentionRepository.VISIBILITY_STRING);

        Iterable<Vertex> sentimentTms = getTermMentionRepository().findByVertexId(v.getId(), tmAuths);
        int count = 0;
        for (Vertex tm : sentimentTms) {
            Assert.assertEquals("sent", BcSchema.TERM_MENTION_TYPE.getPropertyValue(tm));
            count++;
        }

        Assert.assertEquals(5, count);
    }
}
