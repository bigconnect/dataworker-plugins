package io.bigconnect.dw.text.summary;

import java.util.List;
import java.util.Map;

public class TextAnalysisResponse {
    public String processed_text;
    public String original_language;
    public Map<String, Object> sentiment;
    public List<String> topics;
    public String summary;
    public ClassificationResult classification;
    public List<Double> similarity;
    public String error;
}