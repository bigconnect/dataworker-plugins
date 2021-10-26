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
package io.bigconnect.dw.text.extractor;

import com.google.common.base.Strings;
import com.google.inject.Inject;
import com.mware.core.ingest.dataworker.DataWorker;
import com.mware.core.ingest.dataworker.DataWorkerData;
import com.mware.core.ingest.dataworker.DataWorkerPrepareData;
import com.mware.core.ingest.dataworker.ElementOrPropertyStatus;
import com.mware.core.model.Description;
import com.mware.core.model.Name;
import com.mware.core.model.file.FileSystemRepository;
import com.mware.core.model.properties.BcSchema;
import com.mware.core.model.properties.RawObjectSchema;
import com.mware.core.model.workQueue.Priority;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.ge.Element;
import com.mware.ge.Property;
import com.mware.ge.Vertex;
import com.mware.ge.Visibility;
import com.mware.ge.metric.PausableTimerContext;
import com.mware.ge.metric.Timer;
import com.mware.ge.mutation.ExistingElementMutation;
import com.mware.ge.values.storable.*;
import de.l3s.boilerpipe.BoilerpipeProcessingException;
import de.l3s.boilerpipe.extractors.ArticleExtractor;
import de.l3s.boilerpipe.extractors.NumWordsRulesExtractor;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TemporaryResources;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.CompositeParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.pdf.BcParserConfig;
import org.apache.tika.parser.pdf.PDFParserConfig;
import org.apache.tika.sax.BodyContentHandler;
import org.apache.tika.sax.SecureContentHandler;
import org.json.JSONException;
import org.json.JSONObject;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import java.io.*;
import java.nio.charset.Charset;
import java.text.Normalizer;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * By default raw properties will be text extracted into "text" with a text description of "Extracted Text".
 *
 * Configuration:
 *
 * <pre><code>
 * TikaTextExtractorWorker.textExtractMapping.prop1.rawPropertyName=prop1
 * TikaTextExtractorWorker.textExtractMapping.prop1.extractedTextPropertyName=prop1
 * TikaTextExtractorWorker.textExtractMapping.prop1.textDescription=My Property 1
 *
 * TikaTextExtractorWorker.textExtractMapping.prop2.rawPropertyName=prop2
 * TikaTextExtractorWorker.textExtractMapping.prop2.extractedTextPropertyName=prop2
 * TikaTextExtractorWorker.textExtractMapping.prop2.textDescription=My Property 2
 * </code></pre>
 */
@Name("Tika Text Extractor")
@Description("Uses Apache Tika to extract text")
public class TikaTextExtractorWorker extends DataWorker {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(TikaTextExtractorWorker.class);

    @Deprecated
    public static final String MULTI_VALUE_KEY = TikaTextExtractorWorker.class.getName();

    private static final String PROPS_FILE = "tika-extractor.properties";
    private static final String DATE_KEYS_PROPERTY = "tika.extraction.datekeys";
    private static final String SUBJECT_KEYS_PROPERTY = "tika.extraction.titlekeys";
    private static final String AUTHOR_PROPERTY = "tika.extractions.author";
    private static final String URL_KEYS_PROPERTY = "tika.extraction.urlkeys";
    private static final String TYPE_KEYS_PROPERTY = "tika.extraction.typekeys";
    private static final String EXT_URL_KEYS_PROPERTY = "tika.extraction.exturlkeys";
    private static final String SRC_TYPE_KEYS_PROPERTY = "tika.extraction.srctypekeys";
    private static final String RETRIEVAL_TIMESTAMP_KEYS_PROPERTY = "tika.extraction.retrievaltimestampkeys";
    private static final String CUSTOM_FLICKR_METADATA_KEYS_PROPERTY = "tika.extraction.customflickrmetadatakeys";
    private static final String NUMBER_OF_PAGES_PROPERTY = "tika.extraction.numberofpageskeys";

    private static final double SYSTEM_ASSIGNED_CONFIDENCE = 0.4;

    private final TikaTextExtractorWorkerConfiguration configuration;
    private final FileSystemRepository fileSystemRepository;

    private List<String> dateKeys;
    private List<String> subjectKeys;
    private List<String> urlKeys;
    private List<String> typeKeys;
    private List<String> extUrlKeys;
    private List<String> srcTypeKeys;
    private List<String> retrievalTimestampKeys;
    private List<String> customFlickrMetadataKeys;
    private List<String> authorKeys;
    private List<String> numberOfPagesKeys;
    private Timer detectTimer;

    @Inject
    public TikaTextExtractorWorker(
            TikaTextExtractorWorkerConfiguration configuration,
            FileSystemRepository fileSystemRepository) {
        this.configuration = configuration;
        this.fileSystemRepository = fileSystemRepository;
    }

    @Override
    public void prepare(DataWorkerPrepareData workerPrepareData) throws Exception {
        super.prepare(workerPrepareData);

        // TODO: Create an actual properties class?
        Properties tikaProperties = new Properties();
        try {
            // don't require the properties file
            InputStream propsIn = Thread.currentThread().getContextClassLoader().getResourceAsStream(PROPS_FILE);
            if (propsIn != null) {
                tikaProperties.load(propsIn);
            }
        } catch (IOException e) {
            LOGGER.error("Could not load config: %s", PROPS_FILE);
        }

        dateKeys = Arrays.asList(tikaProperties.getProperty(DATE_KEYS_PROPERTY, "date,published,pubdate,publish_date,last-modified,atc:last-modified").split(","));
        subjectKeys = Arrays.asList(tikaProperties.getProperty(SUBJECT_KEYS_PROPERTY, "title,subject").split(","));
        urlKeys = Arrays.asList(tikaProperties.getProperty(URL_KEYS_PROPERTY, "url,og:url").split(","));
        typeKeys = Arrays.asList(tikaProperties.getProperty(TYPE_KEYS_PROPERTY, "Content-Type").split(","));
        extUrlKeys = Arrays.asList(tikaProperties.getProperty(EXT_URL_KEYS_PROPERTY, "atc:result-url").split(","));
        srcTypeKeys = Arrays.asList(tikaProperties.getProperty(SRC_TYPE_KEYS_PROPERTY, "og:type").split(","));
        retrievalTimestampKeys = Arrays.asList(tikaProperties.getProperty(RETRIEVAL_TIMESTAMP_KEYS_PROPERTY, "atc:retrieval-timestamp").split(","));
        customFlickrMetadataKeys = Arrays.asList(tikaProperties.getProperty(CUSTOM_FLICKR_METADATA_KEYS_PROPERTY, "Unknown tag (0x9286)").split(","));
        authorKeys = Arrays.asList(tikaProperties.getProperty(AUTHOR_PROPERTY, "author").split(","));
        numberOfPagesKeys = Arrays.asList(tikaProperties.getProperty(NUMBER_OF_PAGES_PROPERTY, "xmpTPg:NPages").split(","));
        detectTimer = getGraph().getMetricsRegistry().getTimer(getClass(), "extract-time");
    }

    @Override
    public void execute(InputStream in, DataWorkerData data) throws Exception {
        LOGGER.info("Started Tika text extractor");

        final Element element = data.getElement();
        final Property raw = BcSchema.RAW.getProperty(element);
        final TextValue mimeType =
                (TextValue)raw.getMetadata().getValue(BcSchema.MIME_TYPE.getPropertyName());
        checkNotNull(mimeType, BcSchema.MIME_TYPE.getPropertyName() + " is a required metadata field");

        final Charset charset = Charset.forName("UTF-8");
        final Metadata metadata = new Metadata();
        metadata.set(Metadata.CONTENT_TYPE, mimeType.stringValue());

        StreamingPropertyValue rawValue = (StreamingPropertyValue) raw.getValue();
        PausableTimerContext t = new PausableTimerContext(detectTimer);
        String text = extractText(rawValue.getInputStream(), mimeType.stringValue(), metadata);
        t.stop();

        final String propertyKey = raw.getKey();
        TikaTextExtractorWorkerConfiguration.TextExtractMapping textExtractMapping
                                        = configuration.getTextExtractMapping(data.getProperty());

        ExistingElementMutation<Vertex> m = refresh(element).prepareMutation();

        final String author = extractTextField(metadata, authorKeys);
        if (!StringUtils.isEmpty(author)) {
            m.addPropertyValue(propertyKey, RawObjectSchema.AUTHOR.getPropertyName(), Values.stringValue(author),
                    data.createPropertyMetadata(getUser()), data.getProperty().getVisibility());
        }

        final String customImageMetadata = extractTextField(metadata, customFlickrMetadataKeys);
        com.mware.ge.Metadata textMetadata = data.createPropertyMetadata(getUser());
        final Visibility defaultVisibility = getVisibilityTranslator().getDefaultVisibility();
        BcSchema.MIME_TYPE_METADATA.setMetadata(textMetadata, "text/plain", defaultVisibility);
        if (!Strings.isNullOrEmpty(textExtractMapping.getTextDescription())) {
            BcSchema.TEXT_DESCRIPTION_METADATA.setMetadata(
                    textMetadata,
                    textExtractMapping.getTextDescription(),
                    defaultVisibility
            );
        }

        if (customImageMetadata != null && !customImageMetadata.equals("")) {
            try {
                JSONObject customImageMetadataJson = new JSONObject(customImageMetadata);

                text = new JSONObject(customImageMetadataJson.get("description").toString()).get("_content") +
                        "\n" + customImageMetadataJson.get("tags").toString();
                StreamingPropertyValue textValue
                        = new DefaultStreamingPropertyValue(new ByteArrayInputStream(text.getBytes(charset)), StringValue.class);
                addTextProperty(textExtractMapping, m, propertyKey, textValue, textMetadata, data.getProperty().getVisibility());

                ZonedDateTime lastUpdate = GenericDateExtractor
                        .extractSingleDate(customImageMetadataJson.get("lastupdate").toString());
                BcSchema.MODIFIED_DATE.setProperty(m, lastUpdate, defaultVisibility);

                com.mware.ge.Metadata titleMetadata = data.createPropertyMetadata(getUser());
                m.addPropertyValue(propertyKey, BcSchema.TITLE.getPropertyName(),
                        Values.stringValue(customImageMetadataJson.get("title").toString()), titleMetadata, data.getProperty().getVisibility());
            } catch (JSONException e) {
                LOGGER.warn("Image returned invalid custom metadata");
            }
        } else {
            StreamingPropertyValue textValue = new DefaultStreamingPropertyValue(new ByteArrayInputStream(text.getBytes(charset)), StringValue.class);
            addTextProperty(textExtractMapping, m, propertyKey, textValue, textMetadata, data.getProperty().getVisibility());

            BcSchema.MODIFIED_DATE.setProperty(m, extractDate(metadata), defaultVisibility);
            String title = extractTextField(metadata, subjectKeys);
            if (!StringUtils.isEmpty(title)) {
                com.mware.ge.Metadata titleMetadata = data.createPropertyMetadata(getUser());
                m.addPropertyValue(propertyKey, BcSchema.TITLE.getPropertyName(), Values.stringValue(title), titleMetadata, data.getProperty().getVisibility());
            }

            String strNumberOfPages = extractTextField(metadata, numberOfPagesKeys);
            if (!StringUtils.isEmpty(strNumberOfPages)) {
                try {
                    int numberOfPages = Integer.parseInt(strNumberOfPages);
                    com.mware.ge.Metadata numberOfPagesMetadata = data.createPropertyMetadata(getUser());
                    RawObjectSchema.PAGE_COUNT.setProperty(m, numberOfPages, numberOfPagesMetadata, data.getProperty().getVisibility());
                } catch (NumberFormatException ex) {
                    // do nothing
                }
            }
        }

        Element e = m.save(getAuthorizations());

        getGraph().flush();

        getWebQueueRepository().broadcastPropertyChange(e, propertyKey, textExtractMapping.getExtractedTextPropertyName(), data.getWorkspaceId());
        getWorkQueueRepository().pushOnDwQueue(
                e,
                propertyKey,
                textExtractMapping.getExtractedTextPropertyName(),
                data.getWorkspaceId(),
                data.getVisibilitySource(),
                Priority.HIGH,
                ElementOrPropertyStatus.UPDATE,
                null
        );

        LOGGER.info("Ended Tika text extractor");
    }

    private void addTextProperty(
            TikaTextExtractorWorkerConfiguration.TextExtractMapping textExtractMapping,
            ExistingElementMutation<Vertex> m,
            String propertyKey,
            StreamingPropertyValue textValue,
            com.mware.ge.Metadata textMetadata,
            Visibility visibility
    ) {
        m.addPropertyValue(propertyKey, textExtractMapping.getExtractedTextPropertyName(), textValue, textMetadata, visibility);
    }

    private String extractText(InputStream in, String mimeType, Metadata metadata) throws IOException, SAXException, TikaException, BoilerpipeProcessingException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IOUtils.copy(in, out);
        byte[] textBytes = out.toByteArray();
        String text;

        metadata.set(Metadata.CONTENT_TYPE, mimeType);
        String bodyContent = extractTextWithTika(textBytes, metadata);

        if (isHtml(mimeType)) {
            text = extractTextFromHtml(IOUtils.toString(textBytes, "UTF-8"));
            if (text == null || text.length() == 0) {
                text = cleanExtractedText(bodyContent);
            }
        } else {
            text = cleanExtractedText(bodyContent);
        }

        return Normalizer.normalize(text, Normalizer.Form.NFC);
    }

    private static String extractTextWithTika(byte[] textBytes, Metadata metadata) throws TikaException, SAXException, IOException {
        TikaConfig tikaConfig = TikaConfig.getDefaultConfig();
        CompositeParser compositeParser = new CompositeParser(tikaConfig.getMediaTypeRegistry(), tikaConfig.getParser());
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        OutputStreamWriter writer = new OutputStreamWriter(output, "UTF-8");
        ContentHandler handler = new BodyContentHandler(writer);
        ParseContext context = new ParseContext();
        context.set(PDFParserConfig.class, new BcParserConfig());
        ByteArrayInputStream stream = new ByteArrayInputStream(textBytes);

        TemporaryResources tmp = new TemporaryResources();
        try {
            TikaInputStream tis = TikaInputStream.get(stream, tmp);

            // TIKA-216: Zip bomb prevention
            SecureContentHandler sch = new SecureContentHandler(handler, tis);
            try {
                compositeParser.parse(tis, sch, metadata, context);
            } catch (SAXException e) {
                // Convert zip bomb exceptions to TikaExceptions
                sch.throwIfCauseOf(e);
                throw e;
            }
        } finally {
            tmp.dispose();
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("extracted %d bytes", output.size());
            LOGGER.debug("metadata");
            for (String metadataName : metadata.names()) {
                LOGGER.debug("  %s: %s", metadataName, metadata.get(metadataName));
            }
        }
        return IOUtils.toString(output.toByteArray(), "UTF-8");
    }

    private String extractTextFromHtml(String text) throws BoilerpipeProcessingException {
        String extractedText;

        text = cleanHtml(text);

        extractedText = NumWordsRulesExtractor.getInstance().getText(text);
        if (extractedText != null && extractedText.length() > 0) {
            return extractedText;
        }

        extractedText = ArticleExtractor.getInstance().getText(text);
        if (extractedText != null && extractedText.length() > 0) {
            return extractedText;
        }

        return null;
    }

    private String cleanHtml(String text) {
        text = text.replaceAll("&mdash;", "--");
        text = text.replaceAll("&ldquo;", "\"");
        text = text.replaceAll("&rdquo;", "\"");
        text = text.replaceAll("&lsquo;", "'");
        text = text.replaceAll("&rsquo;", "'");
        return text;
    }

    private ZonedDateTime extractDate(Metadata metadata) {
        // find the date metadata property, if there is one
        String dateKey = TikaMetadataUtils.findKey(dateKeys, metadata);
        ZonedDateTime date = null;
        if (dateKey != null) {
            date = GenericDateExtractor
                    .extractSingleDate(metadata.get(dateKey));
        }

        if (date == null) {
            date = ZonedDateTime.now();
        }

        return date;
    }

    private String extractTextField(Metadata metadata, List<String> keys) {
        // find the title metadata property, if there is one
        String field = "";
        String fieldKey = TikaMetadataUtils.findKey(keys, metadata);

        if (fieldKey != null) {
            field = metadata.get(fieldKey);
        }

        if (field != null) {
            field = field.trim();
        }
        return field;
    }

    private boolean isHtml(String mimeType) {
        return mimeType.contains("html");
    }

    private String cleanExtractedText(String extractedText) {
        return extractedText
                // Normalize line breaks
                .replaceAll("\r", "\n")
                // Remove tabs
                .replaceAll("\t", " ")
                // Remove non-breaking spaces
                .replaceAll("\u00A0", " ")
                // Remove newlines that are just paragraph wrapping
                .replaceAll("(?<![\\n])[\\n](?![\\n])", " ")
                // Remove remaining newlines with exactly 2
                .replaceAll("([ ]*\\n[ ]*)+", "\n\n")
                // Remove duplicate spaces
                .replaceAll("[ ]+", " ");
    }

    @Override
    public boolean isHandled(Element element, Property property) {
        if (property == null) {
            return false;
        }

        String mimeType = BcSchema.MIME_TYPE.getFirstPropertyValue(element);
        if (mimeType == null || mimeType.startsWith("image") || mimeType.startsWith("video") || mimeType.startsWith("audio")) {
            return false;
        }

        if (BcSchema.RAW.getProperty(element) == null)
            return false;

        return configuration.isHandled(element, property);
    }
}

