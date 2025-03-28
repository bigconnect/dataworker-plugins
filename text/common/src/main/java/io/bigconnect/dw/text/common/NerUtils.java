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
package io.bigconnect.dw.text.common;

import com.mware.core.model.termMention.TermMentionRepository;
import com.mware.core.model.termMention.TermMentionUtils;
import com.mware.ge.Authorizations;
import com.mware.ge.Graph;
import com.mware.ge.Vertex;
import org.apache.commons.lang.StringUtils;
import org.apache.poi.ss.formula.functions.T;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NerUtils {
    public static synchronized void removeEntityTermMentions(
            Vertex outVertex,
            TermMentionRepository termMentionRepository,
            TermMentionUtils termMentionUtils,
            Graph graph,
            Authorizations authorizations
    ) {
        // delete existing term mentions
        termMentionRepository.deleteTermMentions("", outVertex.getId(), authorizations);
        termMentionRepository.deleteTermMentions("ent", outVertex.getId(), authorizations);
        termMentionUtils.removeHasDetectedEntityRelations(outVertex);
        graph.flush();
    }

    public static synchronized void removeSentimentTermMentions(
            Vertex outVertex,
            TermMentionRepository termMentionRepository,
            Graph graph,
            Authorizations authorizations
    ) {
        // delete existing term mentions
        termMentionRepository.deleteTermMentions("sent", outVertex.getId(), authorizations);
        graph.flush();
    }

    private static final String BLANK_LINE = "(?<=(\r\n|\r|\n))([ \\t]*$)+";
    private static final Pattern paragraphSplitter = Pattern.compile(BLANK_LINE, Pattern.MULTILINE);

    public static List<TextSpan> getParagraphs(String text) {
        if (StringUtils.isEmpty(text))
            return Collections.emptyList();

        List<TextSpan> result = new ArrayList<>();
        Matcher m = paragraphSplitter.matcher(text);
        int start = 0;
        while (m.find()) {
            int end = m.start();
            String found = text.subSequence(start, end).toString();
            if (!StringUtils.isEmpty(found.trim())) {
                int innerStart = 0, innerEnd = found.length();
                // skip start and end CR LF
                char[] arr = found.toCharArray();
                for (int i = 0; i < arr.length; i++) {
                    // skip start
                    if (arr[i] == '\n' || arr[i] == '\r') {
                        start++;
                        innerStart++;
                    }
                    else break;
                }
                for (int i = arr.length - 1; i >= 0; i--) {
                    // skip end
                    if (arr[i] == '\n' || arr[i] == '\r') {
                        end--;
                        innerEnd--;
                    }
                    else break;
                }
                TextSpan ann = new TextSpan(start, end, found.substring(innerStart, innerEnd));
                result.add(ann);
                start = m.end();
            }
        }

        if (start < text.length()) {
            result.add(new TextSpan(start, text.length(), text.substring(start)));
        }

        if (result.isEmpty() && text.trim().length() > 0)
            result.add(new TextSpan(0, text.length(), text));

        return result;
    }

    /**
     * Splits text into chunks optimized for all-MiniLM-L6-v2 model while
     * respecting natural language boundaries like sentences and named entities
     * @param text The original text
     * @return List of TextSpan objects with model-friendly chunks
     */
    public static List<TextSpan> getSmartMiniLMChunks(String text) {
        // all-MiniLM-L6-v2 has 256 token limit
        // Conservative estimate of ~3 chars per token
        int maxChars = 256 * 3;

        List<TextSpan> chunks = new ArrayList<>();

        // Normalize the text - replace line breaks with spaces to create continuous text
        String normalizedText = text.replaceAll("\\r?\\n", " ")
                .replaceAll("\\s+", " ")
                .trim();

        if (normalizedText.isEmpty()) {
            return chunks;
        }

        // If the text is already short enough, return it as a single chunk
        if (normalizedText.length() <= maxChars) {
            chunks.add(new TextSpan(0, text.length(), normalizedText));
            return chunks;
        }

        // First, split into sentences
        List<TextSpan> sentences = new ArrayList<>();
        Pattern sentencePattern = Pattern.compile("(.*?[.!?])(\\s|$)");
        Matcher sentenceMatcher = sentencePattern.matcher(normalizedText);

        int lastEnd = 0;
        while (sentenceMatcher.find()) {
            String sentence = sentenceMatcher.group(1);
            int start = sentenceMatcher.start();
            int end = sentenceMatcher.end();
            sentences.add(new TextSpan(start, end, sentence + (end < normalizedText.length() ? " " : "")));
            lastEnd = end;
        }

        // Handle any trailing text without punctuation
        if (lastEnd < normalizedText.length()) {
            sentences.add(new TextSpan(lastEnd, normalizedText.length(),
                    normalizedText.substring(lastEnd)));
        }

        // Identify potential named entities to avoid splitting them
        // Simple pattern for common entity markers (could be enhanced with a real NER model)
        Pattern entityPattern = Pattern.compile("([A-Z][a-z]+(\\s[A-Z][a-z]+)+)");
        List<TextSpan> entities = new ArrayList<>();

        for (TextSpan sentence : sentences) {
            Matcher entityMatcher = entityPattern.matcher(sentence.getText());
            while (entityMatcher.find()) {
                int entityStart = sentence.getStart() + entityMatcher.start();
                int entityEnd = sentence.getStart() + entityMatcher.end();
                entities.add(new TextSpan(entityStart, entityEnd, entityMatcher.group(1)));
            }
        }

        // Now combine sentences into chunks respecting entity boundaries
        StringBuilder currentChunk = new StringBuilder();
        int chunkStart = 0;

        for (TextSpan sentence : sentences) {
            // If adding this sentence would exceed max length, finalize current chunk
            if (currentChunk.length() + sentence.getText().length() > maxChars && currentChunk.length() > 0) {
                chunks.add(new TextSpan(chunkStart, chunkStart + currentChunk.length(),
                        currentChunk.toString().trim()));
                currentChunk = new StringBuilder();
                chunkStart = sentence.getStart();
            }

            // Handle very long sentences that exceed max length on their own
            if (sentence.getText().length() > maxChars) {
                // If we have content in the current chunk, finalize it
                if (currentChunk.length() > 0) {
                    chunks.add(new TextSpan(chunkStart, chunkStart + currentChunk.length(),
                            currentChunk.toString().trim()));
                    currentChunk = new StringBuilder();
                }

                // Split the long sentence into multiple chunks, respecting entity boundaries
                String longSentence = sentence.getText();
                int sentenceStart = sentence.getStart();
                int chunkEnd = 0;

                for (int i = 0; i < longSentence.length(); i = chunkEnd) {
                    // Determine where to end this chunk
                    chunkEnd = Math.min(i + maxChars, longSentence.length());

                    // Don't break in the middle of a word
                    if (chunkEnd < longSentence.length()) {
                        int lastSpace = longSentence.lastIndexOf(' ', chunkEnd);
                        if (lastSpace > i) {
                            chunkEnd = lastSpace + 1;
                        }
                    }

                    // Check if we'd be breaking in the middle of a named entity
                    boolean breakingEntity = false;
                    for (TextSpan entity : entities) {
                        if (sentenceStart + chunkEnd > entity.getStart() &&
                                sentenceStart + chunkEnd < entity.getEnd()) {
                            // We'd break this entity, adjust to the entity's start
                            chunkEnd = entity.getStart() - sentenceStart;
                            if (chunkEnd <= i) {
                                // Entity starts before or at our current position,
                                // so include the whole entity
                                chunkEnd = entity.getEnd() - sentenceStart;
                                if (chunkEnd - i > maxChars) {
                                    // Entity is too long, just break at a word boundary
                                    chunkEnd = Math.min(i + maxChars, longSentence.length());
                                    int lastSpace = longSentence.lastIndexOf(' ', chunkEnd);
                                    if (lastSpace > i) {
                                        chunkEnd = lastSpace + 1;
                                    }
                                }
                            }
                            breakingEntity = true;
                            break;
                        }
                    }

                    // Create the chunk
                    String subSentence = longSentence.substring(i, chunkEnd).trim();
                    if (!subSentence.isEmpty()) {
                        chunks.add(new TextSpan(sentenceStart + i, sentenceStart + chunkEnd, subSentence));
                    }
                }

                continue;
            }

            // Start new chunk if needed
            if (currentChunk.length() == 0) {
                chunkStart = sentence.getStart();
            }

            // Add sentence to current chunk
            currentChunk.append(sentence.getText());
        }

        // Add any remaining text as the final chunk
        if (currentChunk.length() > 0) {
            chunks.add(new TextSpan(chunkStart, chunkStart + currentChunk.length(),
                    currentChunk.toString().trim()));
        }

        return chunks;
    }


}
