package io.bigconnect.dw.sentiment.intellidockers;

public class SentimentRequest {
    public String content;
    public String language;

    public SentimentRequest(String content, String language) {
        this.content = content;
        this.language = language;
    }
}
