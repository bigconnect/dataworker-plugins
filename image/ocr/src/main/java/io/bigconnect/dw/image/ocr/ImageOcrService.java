package io.bigconnect.dw.image.ocr;

import okhttp3.MultipartBody;
import retrofit2.Call;
import retrofit2.http.Header;
import retrofit2.http.Multipart;
import retrofit2.http.POST;
import retrofit2.http.Part;

public interface ImageOcrService {
    @Multipart
    @POST("/v1/ocr")
    Call<ImageOcrResponse> process(
            @Part MultipartBody.Part file
    );
}

