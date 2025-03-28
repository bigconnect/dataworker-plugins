package io.bigconnect.dw.classification.iptc.intellidockersVllm;

import lombok.Getter;

import java.util.List;
import java.util.Map;

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
        this.language = language;
        this.num_topics = 5;
    }
}

