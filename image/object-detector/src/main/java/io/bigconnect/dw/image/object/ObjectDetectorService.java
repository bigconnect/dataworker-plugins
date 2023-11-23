package io.bigconnect.dw.image.object;

import retrofit2.Call;
import retrofit2.http.Body;

public interface ObjectDetectorService {
    Call<ObjectDetectorResponse> process(@Body byte[] image);
}
