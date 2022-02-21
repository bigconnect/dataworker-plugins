package io.bigconnect.dw.ner.common.extractor;

import com.bericotech.clavin.extractor.LocationOccurrence;

public class ExtLocationOccurence extends LocationOccurrence {
    public int sentiment = 0;
    public double sentimentScore = 0.0d;

    public ExtLocationOccurence(String text, int position, int sentiment, double sentimentScore) {
        this(text, position);
        this.sentiment = sentiment;
        this.sentimentScore = sentimentScore;
    }

    public ExtLocationOccurence(String text, int position) {
        super(text, position);
    }
}
