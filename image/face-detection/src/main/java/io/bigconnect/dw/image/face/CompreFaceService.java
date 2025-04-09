package io.bigconnect.dw.image.face;

import okhttp3.MultipartBody;
import okhttp3.RequestBody;
import retrofit2.Call;
import retrofit2.http.*;

import java.util.List;

public interface CompreFaceService {
    @POST("/api/v1/recognition/subjects")
    @Headers({
            "Content-Type: application/json"
    })
    Call<Void> createSubject(@Header("X-Api-Key") String apiKey, @Body SubjectRequest request);

    @Multipart
    @POST("/api/v1/recognition/recognize")
    Call<RecognitionResponse> recognizeFace(
            @Header("X-Api-Key") String apiKey,
            @Part MultipartBody.Part file,
            @Query("face_plugins") List<String> facePlugins  // Changed to List<String>
    );
    @Multipart
    @POST("/api/v1/recognition/faces")
    Call<EmbeddingResponse> addFaceToSubject(
            @Header("X-Api-Key") String apiKey,
            @Part MultipartBody.Part file,  // Changed this
            @Query("subject") String subject
    );
    @DELETE("api/v1/recognition/subjects/{subject}")
    Call<Void> deleteSubject(@Header("X-Api-Key") String apiKey, @Path("subject") String subject);

    @GET("api/v1/recognition/subjects")
    Call<SubjectsResponse> getSubjects(@Header("X-Api-Key") String apiKey);

}
