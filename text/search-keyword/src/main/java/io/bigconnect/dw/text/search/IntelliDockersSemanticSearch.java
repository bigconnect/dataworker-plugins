package io.bigconnect.dw.text.search;

import retrofit2.Call;
import retrofit2.http.Body;
import retrofit2.http.GET;
import retrofit2.http.POST;

public interface IntelliDockersSemanticSearch {
    @GET("rest/ready")
    Call<String> ready();

    @POST("/combined-search")
    Call<CombinedSearchResponse> combinedSearch(@Body CombinedSearchRequest request);
}