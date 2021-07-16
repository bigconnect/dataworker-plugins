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
        if (result.isEmpty() && text.trim().length() > 0)
            result.add(new TextSpan(0, text.length(), text));

        return result;
    }

    public static void main(String[] args) {
        List<TextSpan> p = getParagraphs("\r\nSentence1\nSentence2\n\nSentence3\nSentence4\r\nSentence5\r\n\r\n");
        System.out.println(p.size());
    }
}
