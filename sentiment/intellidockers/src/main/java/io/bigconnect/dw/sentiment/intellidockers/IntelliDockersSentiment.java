package io.bigconnect.dw.sentiment.intellidockers;

import retrofit2.Call;
import retrofit2.http.Body;
import retrofit2.http.POST;

public interface IntelliDockersSentiment {
    @POST("rest/process")
    Call<SentimentResponse> process(@Body SentimentRequest request);
}
