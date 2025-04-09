package io.bigconnect.dw.image.face;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.google.gson.annotations.SerializedName;
import lombok.Data;

import java.util.List;

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SubjectsResponse {

    @SerializedName("subjects")
    private List<String> subjects;

}
