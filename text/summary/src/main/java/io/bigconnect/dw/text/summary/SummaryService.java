package io.bigconnect.dw.text.summary;

import retrofit2.Call;
import retrofit2.http.Body;
import retrofit2.http.GET;
import retrofit2.http.POST;

public interface SummaryService {
    @POST("rest/process")
    Call<SummaryResponse> process(@Body SummaryRequest request);

    @GET("rest/ready")
    Call<String> ready();
}
