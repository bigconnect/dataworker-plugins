package io.bigconnect.dw.image.ocr;


import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;

@Getter
public class ImageOcrResponse {
    private String status;
    private Results results;


    @Getter
    public static class Results {
        @JsonProperty("english_text")
        private String englishText;

        @JsonProperty("romanian_text")
        private String romanianText;

        @JsonProperty("best_combined")
        private String bestCombined;

        private long timestamp;

    }

}