package io.bigconnect.dw.image.face;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import java.util.List;

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class RecognitionResponse {
    public List<Result> result;

    @Data
    public static class Result {
        public Age age;
        public Box box;
        public Gender gender;
        public List<List<Integer>> landmarks;
        public Pose pose;
        public List<Subject> subjects;

        @Data
        public static class Age {
            public Double probability;
            public Integer high;
            public Integer low;
        }

        @Data
        public static class Box {
            public Double probability;
            public Integer x_max;
            public Integer x_min;
            public Integer y_max;
            public Integer y_min;
        }

        @Data
        public static class Gender {
            public Double probability;
            public String value;
        }

        @Data
        public static class Pose {
            public Double pitch;
            public Double roll;
            public Double yaw;
        }

        @Data
        public static class Subject {
            public String subject;
            public Double similarity;
        }
    }
}