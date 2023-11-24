package io.bigconnect.dw.image.face;

import okhttp3.RequestBody;
import retrofit2.Call;
import retrofit2.http.Body;
import retrofit2.http.POST;

public interface FaceDetectorService {
    @POST("process")
    Call<FaceDetectorResponse> process(@Body RequestBody image);
}
