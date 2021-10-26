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
package io.bigconnect.dw.sentiment.corenlp;

import com.mware.core.ingest.dataworker.DataWorker;
import com.mware.core.ingest.dataworker.DataWorkerData;
import com.mware.core.ingest.dataworker.DataWorkerPrepareData;
import com.mware.core.ingest.dataworker.ElementOrPropertyStatus;
import com.mware.core.model.Description;
import com.mware.core.model.Name;
import com.mware.core.model.properties.BcSchema;
import com.mware.core.model.properties.RawObjectSchema;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.ge.Element;
import com.mware.ge.Property;
import com.mware.ge.Vertex;
import com.mware.ge.Visibility;
import com.mware.ge.mutation.ElementMutation;
import com.mware.ge.mutation.ExistingElementMutation;
import com.mware.ge.values.storable.StreamingPropertyValue;
import com.mware.ge.values.storable.Values;
import com.mware.ontology.IgnoredMimeTypes;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@Name("Stanford CoreNLP Sentiment Analysis")
@Description("Extracts sentiment from text using Stanford CoreNLP")
public class SentimentAnalysisExtractorWorker extends DataWorker {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(SentimentAnalysisExtractorWorker.class);

    private StanfordCoreNLP pipeline;

    @Override
    public void prepare(DataWorkerPrepareData workerPrepareData) throws Exception {
        super.prepare(workerPrepareData);
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, parse, sentiment");
        pipeline = new StanfordCoreNLP(props);
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
            return !StringUtils.isEmpty(language);
        }

        return false;
    }

    @Override
    public void execute(InputStream in, DataWorkerData data) throws Exception {
        String language = RawObjectSchema.RAW_LANGUAGE.getPropertyValue(data.getProperty());
        StreamingPropertyValue textProperty = BcSchema.TEXT.getPropertyValue(refresh(data.getElement()), data.getProperty().getKey());

        if (textProperty == null) {
            LOGGER.warn("Could not find text property for language: " + language);
            return;
        }

        String text = IOUtils.toString(textProperty.getInputStream(), StandardCharsets.UTF_8);

        ElementMutation m = refresh(data.getElement()).prepareMutation();
        m.deleteProperty(RawObjectSchema.RAW_SENTIMENT.getPropertyName(), Visibility.EMPTY);
        Element element = m.save(getAuthorizations());
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

        String sentiment = getSentiment(text);
        ExistingElementMutation<Vertex> mutation = refresh(data.getElement()).prepareMutation();
        com.mware.ge.Metadata metadata = data.createPropertyMetadata(getUser());
        mutation.setProperty(RawObjectSchema.RAW_SENTIMENT.getPropertyName(), Values.stringValue(sentiment), metadata, data.getVisibility());
        Element e = mutation.save(getAuthorizations());
        getGraph().flush();

        getWorkQueueRepository().pushOnDwQueue(
                e,
                "",
                RawObjectSchema.RAW_SENTIMENT.getPropertyName(),
                data.getWorkspaceId(),
                null,
                data.getPriority(),
                ElementOrPropertyStatus.UPDATE,
                null);
    }

    private String getSentiment(String text) {
        List<Integer> sentiments = new ArrayList<>();
        List<Integer> sizes = new ArrayList<>();
        Annotation annotation = pipeline.process(text);
        for (CoreMap sentence : annotation.get(CoreAnnotations.SentencesAnnotation.class)) {
            Tree tree = sentence.get(SentimentCoreAnnotations.AnnotatedTree.class);
            int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
            sentiments.add(sentiment);
            sizes.add(tree.toString().length());

        }

        int weightedSentiment;

        if (sentiments.isEmpty())
            weightedSentiment = -1;
        else {
            List<Integer> weightedSentiments = new ArrayList<>();
            for (int i = 0; i < sentiments.size(); i++) {
                int sent = sentiments.get(i);
                int size = sizes.get(i);
                weightedSentiments.add(sent * size);
            }
            weightedSentiment = weightedSentiments.stream().mapToInt(i -> i).sum() / sizes.stream().mapToInt(i -> i).sum();
        }

        return normalizeCoreNLPSentiment(weightedSentiment);
    }

    private String normalizeCoreNLPSentiment(double sentiment) {
        if (sentiment <= 0) return "neutral";
        else if (sentiment < 2.0) return "negative";
        else if (sentiment < 3.0) return "neutral";
        else if (sentiment < 5.0) return "positive";
        else return "neutral";
    }

    public static void main(String[] args) throws Exception {
        SentimentAnalysisExtractorWorker worker = new SentimentAnalysisExtractorWorker();
        worker.prepare(null);
        String result = worker.getSentiment("MANILA, Philippines – Rappler is one of 3 organizations that will be recognized by the US-based National Democratic Institute (NDI) for their work in fighting disinformation and fake news. NDI said Rappler will receive its highest honor – the W. Averell Harriman Democracy Award. NDI is chaired by Madeleine Albright, the 64th US secretary of state, the first woman to become America's top diplomat. Past recipients of this award include former UN secretary general Kofi Annan, first Czech Republic president Václav Havel, Nobel Peace Prize laureate Aung San Suu Kyi, former US presidents Jimmy Carter and Bill Clinton, first female African head of state Ellen Johnson Sirleaf, and Archbishop Desmond Tutu of South Africa. In an announcement on October 26, NDI explained that Rappler \"has suffered threats and severe internal pressure for its pioneering work in exposing disinformation and propaganda in the Philippines to manipulate public opinion.\" \"The story of Rappler shows how the use of disinformation and computational propaganda are bleeding over to domestic actors in new and consolidated democracies, resulting in democratic backsliding,\" NDI said. In October 2016 – months after the 2016 presidential elections in the Philippines – Rappler ran a 3-part series on fake news and trolls who spread propaganda on social media. (READ: Propaganda war: Weaponizing the internet) \"Attacks on Rappler's founder also demonstrate the particularly vicious ways in which disinformation has been used to attack women who are active in political life,\" NDI noted. Rappler CEO and Executive Editor Maria Ressa will accept the award during NDI's annual Democracy Dinner on November 2. The award comes after Rappler officially became a signatory member of the International Fact-Checking Network (IFCN) of Poynter, a forum for fact checkers worldwide. Read NDI's description of Rappler below: Rappler is an online social news network based in the Philippines. It holds public and private sectors accountable, pursuing truth and transparency for the people served. It encourages its readership to be aware of the spread of disinformation and propaganda, and exposes the hidden social media \"machines\" or bots that distort the truth. Rappler has suffered threats and severe internal pressure for its pioneering work in exposing disinformation and propaganda in the Philippines to manipulate public opinion. The story of Rappler shows how the use of disinformation and computational propaganda are bleeding over to domestic actors in new and consolidated democracies, resulting in democratic backsliding. Attacks on Rappler's founder also demonstrate the particularly vicious ways in which disinformation has been used to attack women who are active in political life. Maria A. Ressa, who is accepting the award on behalf of the outlet, is the CEO and executive editor of Rappler, and is a former CNN bureau chief and investigative reporter. Aside from Rappler, StopFake.org from Ukraine and The Oxford Internet Institute from the United Kingdom will also be recognized by NDI on November 2. The annual W. Averell Harriman Democracy Award recognizes an individual or organization that has demonstrated a commitment to democracy and human rights. Aside from Rappler, the only other recipient of the award from the Philippines was former President Corazon Aquino, who was recognized by NDI back in 2004. NDI is a nonprofit, nonpartisan organization working to support and strengthen democratic institutions worldwide through citizen participation, openness, and accountability in government. – Rappler.com");
        System.out.println(result);
    }
}
