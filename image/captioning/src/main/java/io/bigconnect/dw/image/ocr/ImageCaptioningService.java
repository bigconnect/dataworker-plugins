package io.bigconnect.dw.image.ocr;

import okhttp3.RequestBody;
import retrofit2.Call;
import retrofit2.http.Body;
import retrofit2.http.POST;

public interface ImageCaptioningService {
    @POST("process")
    Call<ImageCaptioningResponse> process(@Body RequestBody image);
}
