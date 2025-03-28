package io.bigconnect.dw.text.zeroShotClassification;

public class TextRequest {
    private String text;
    private boolean dev;

    public TextRequest(String text, boolean dev) {
        this.text = text;
        this.dev = dev;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public boolean isDev() {
        return dev;
    }

    public void setDev(boolean dev) {
        this.dev = dev;
    }
}