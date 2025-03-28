package io.bigconnect.dw.text.zeroShotClassification;

import retrofit2.Call;
import retrofit2.http.Body;
import retrofit2.http.GET;
import retrofit2.http.POST;

public interface TopicClassifierService {
    @GET("rest/ready")
    Call<String> ready();

    @POST("/classify")
    Call<ClassificationResponse> classifyTopic(@Body TextRequest request);
}