package io.bigconnect.dw.classification.iptc.intellidockers;

public class IptcRequest {
    public String content;
    public String language;

    public IptcRequest() {
    }

    public IptcRequest(String content, String language) {
        this.content = content;
        this.language = language;
    }
}
