package io.bigconnect.dw.image.object;

import okhttp3.RequestBody;
import retrofit2.Call;
import retrofit2.http.Body;
import retrofit2.http.POST;

public interface ObjectDetectorService {
    @POST("process")
    Call<ObjectDetectorResponse> process(@Body RequestBody image);
}
