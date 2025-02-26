package io.bigconnect.dw.image.face;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SubjectRequest {
    @JsonProperty("subject")
    private String subject;

    public SubjectRequest(String subject) {
        this.subject = subject;
    }
}