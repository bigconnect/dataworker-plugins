package io.bigconnect.dw.text.summary;

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

    public TextAnalysisRequest(String text, String analysis_type) {
        this.text = text;
        this.analysis_type = analysis_type;
        this.num_topics = 3; // default value
    }
}

