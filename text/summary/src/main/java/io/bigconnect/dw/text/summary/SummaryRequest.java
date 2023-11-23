package io.bigconnect.dw.text.summary;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
public class SummaryRequest {
    public String content;
    public String language = "ron";
    public int size = 10;

    public SummaryRequest(String content) {
        this.content = content;
    }
}
