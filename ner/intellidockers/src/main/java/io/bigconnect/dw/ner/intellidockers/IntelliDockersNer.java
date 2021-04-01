package io.bigconnect.dw.ner.intellidockers;

import retrofit2.Call;
import retrofit2.http.Body;
import retrofit2.http.POST;

public interface IntelliDockersNer {
    @POST("rest/process")
    Call<Entities> process(@Body NerRequest request);
}
