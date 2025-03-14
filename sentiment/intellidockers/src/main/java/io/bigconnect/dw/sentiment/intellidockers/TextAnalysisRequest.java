package io.bigconnect.dw.sentiment.intellidockers;

import lombok.Getter;

import java.util.List;

@Getter
public class TextAnalysisRequest {
    private String text;
    private String analysis_type;
    private Integer num_topics;
    private Integer max_length;
    private Integer min_length;
    private List<String> labels;
    private String comparison_text;
    private String language;

    public TextAnalysisRequest(String text, String analysis_type, String language) {
        this.text = text;
        this.analysis_type = analysis_type;
        this.num_topics = 3; // default value
        this.language = language;
    }
}

