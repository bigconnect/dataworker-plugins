package io.bigconnect.dw.ner.intellidockers;

import com.drew.lang.Iterables;
import com.mware.core.InMemoryGraphTestBase;
import com.mware.core.ingest.dataworker.DataWorkerData;
import com.mware.core.ingest.dataworker.DataWorkerPrepareData;
import com.mware.core.model.properties.BcSchema;
import com.mware.core.model.properties.RawObjectSchema;
import com.mware.core.model.schema.SchemaConstants;
import com.mware.core.model.workQueue.Priority;
import com.mware.ge.Authorizations;
import com.mware.ge.Vertex;
import com.mware.ge.Visibility;
import com.mware.ge.values.storable.DefaultStreamingPropertyValue;
import io.bigconnect.dw.ner.common.EntityExtractionDataWorker;
import org.apache.commons.io.IOUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockserver.client.server.MockServerClient;
import org.mockserver.integration.ClientAndServer;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

public class TestNerWithSentiment extends InMemoryGraphTestBase {
    private static ClientAndServer mockServer;

    @BeforeClass
    public static void startServer() {
        mockServer = startClientAndServer(24331);
    }

    @AfterClass
    public static void stopServer() {
        mockServer.stop();
    }

    Authorizations AUTHS = new Authorizations();
    String TEXT = "Candidatul independent la Primăria Capitalei susţinut de USR, Nicuşor Dan, afirmă că " +
            "bugetul Primăriei Municipiului Bucureşti pe 2020 'nu are nicio legătură cu realitatea', " +
            "în condiţiile în care veniturile estimate – 7,2 miliarde de lei – sunt, ca şi în anii precedenţi, " +
            "supraevaluate cu aproximativ 75%, transmite Agerpres.";

    @Test
    public void test() throws Exception {
        InputStream is = getClass().getResourceAsStream("/response.json");
        String responseBody = IOUtils.toString(is, StandardCharsets.UTF_8);
        new MockServerClient("localhost", mockServer.getPort())
                .when(request()
                        .withMethod("POST")
                        .withPath("/rest/process")
                        .withHeader("\"Content-type\", \"application/json\"; charset=utf-8")
                ).respond(response()
                        .withStatusCode(200)
                        .withHeader("\"Content-type\", \"application/json\"; charset=utf-8")
                        .withBody(responseBody)
                );

        getConfiguration().set("intellidockers.ron.ner.url", "http://localhost:"+mockServer.getPort());

        Vertex v = getGraph().prepareVertex(Visibility.EMPTY, SchemaConstants.CONCEPT_TYPE_DOCUMENT)
                .save(AUTHS);
        BcSchema.TEXT.addPropertyValue(v, "ro", DefaultStreamingPropertyValue.create(TEXT), Visibility.EMPTY, AUTHS);
        RawObjectSchema.RAW_LANGUAGE.addPropertyValue(v, "ro", "ro", Visibility.EMPTY, AUTHS);

        EntityExtractionDataWorker dw = new EntityExtractionDataWorker(getTermMentionRepository());
        dw.setConfiguration(getConfiguration());
        dw.setGraph(getGraph());
        dw.setVisibilityTranslator(getVisibilityTranslator());
        dw.setWebQueueRepository(getWebQueueRepository());

        dw.prepare(new DataWorkerPrepareData(
                getConfigurationMap(), Collections.emptyList(), getUser(), AUTHS, null
        ));
        DataWorkerData data = new DataWorkerData(
                getVisibilityTranslator(),
                v,
                v.getProperty(RawObjectSchema.RAW_LANGUAGE.getPropertyName()),
                null,
                null,
                Priority.HIGH,
                false
        );
        dw.execute(new ByteArrayInputStream(TEXT.getBytes(StandardCharsets.UTF_8)), data);

        List<Vertex> tms = Iterables.toList(getTermMentionRepository().findByOutVertex(v.getId(), AUTHS));
        for (Vertex tm : tms) {
            Long start = BcSchema.TERM_MENTION_START_OFFSET.getPropertyValue(tm);
            Long end = BcSchema.TERM_MENTION_END_OFFSET.getPropertyValue(tm);
            String title = BcSchema.TERM_MENTION_TITLE.getPropertyValue(tm);
            String style = BcSchema.TERM_MENTION_STYLE.getPropertyValue(tm);
            String concept = BcSchema.TERM_MENTION_CONCEPT_TYPE.getPropertyValue(tm);
            double score = BcSchema.TERM_MENTION_SCORE.getPropertyValue(tm);
            System.out.println(String.format("%s[%d - %d] => %s:%f (%s)", title, start, end, concept, score, style));
        }
    }
}
