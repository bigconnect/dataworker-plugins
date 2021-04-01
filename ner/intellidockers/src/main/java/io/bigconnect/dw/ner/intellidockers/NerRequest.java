package io.bigconnect.dw.ner.intellidockers;

public class NerRequest {
    public String content;
    public String language;

    public NerRequest() {
    }

    public NerRequest(String content, String language) {
        this.content = content;
        this.language = language;
    }
}
