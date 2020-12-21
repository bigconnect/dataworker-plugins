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
package io.bigconnect.dw.ner.common.places.substitutions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.BreakIterator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

public class WikipediaDemonymMap extends AbstractSubstitutionMap {

    private static final Logger logger = LoggerFactory.getLogger(WikipediaDemonymMap.class);

    public static final String RESOURCE_NAME = "wikipedia-demonyms.tsv";

    public WikipediaDemonymMap(){
        this.ignoreCase = false;
        try {
            loadFromFile();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            logger.error("Unable to load demonym list! "+e);
        }
    }

    private void loadFromFile() throws IOException{
        logger.info("Loading demonyms from "+RESOURCE_NAME);
        map = new HashMap<String,String>();
        BufferedReader br = new BufferedReader(new InputStreamReader(this.getClass().getClassLoader().getResourceAsStream(RESOURCE_NAME)));
        // drop the two table header rows
        br.readLine();
        br.readLine();
        // now walk each line
        String row = null;
        while ((row = br.readLine()) != null) {
            // parse country demonym info out of line
           String[] columns = row.split("\t");
           String countryName = columns[0];
           String[] adjectivals = columns[1].split(", ");
           ArrayList<String> demonyms = new ArrayList<String>();
           for(int c=2;c<columns.length;c++){
               demonyms.addAll( Arrays.asList(columns[c].split(", ")) );
           }
           demonyms.addAll( Arrays.asList(adjectivals) );
           // add demonyms to map
           for(String demonym:demonyms){
               put(demonym.trim(), countryName.trim());
               logger.trace("added "+demonym+" to "+countryName);
           }
        }
    }

    /**
     * HACK: this a a big performance hit
     * @param textToParse
     * @return
     */
    public String replaceAll(String textToParse) {
        int demonymsFound = 0;
        BreakIterator boundary = BreakIterator.getWordInstance();
        boundary.setText(textToParse);
        int start = boundary.first();
        StringBuffer cleanedText = new StringBuffer();
        for (int end = boundary.next(); end != BreakIterator.DONE; start=end, end=boundary.next()) {
            String originalWord = textToParse.substring(start,end);
            String cleanedWord = substituteIfNeeded(originalWord);
            if(!cleanedWord.equals(originalWord)){
                demonymsFound++;
                logger.debug("    substituting demonym: "+originalWord+" -> "+cleanedWord);
            }
            cleanedText.append(cleanedWord);
        }
        logger.debug("  Replaced "+demonymsFound+" demonyms");
        return cleanedText.toString();
    }

}

