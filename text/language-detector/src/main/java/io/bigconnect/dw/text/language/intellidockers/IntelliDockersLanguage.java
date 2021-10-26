package io.bigconnect.dw.text.language.intellidockers;

import retrofit2.Call;
import retrofit2.http.Body;
import retrofit2.http.POST;

public interface IntelliDockersLanguage {
    @POST("rest/process")
    Call<IdLanguageResponse> process(@Body IdLanguageRequest request);
}
