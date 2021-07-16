package io.bigconnect.dw.classification.iptc.intellidockers;

import retrofit2.Call;
import retrofit2.http.Body;
import retrofit2.http.POST;

public interface IntelliDockersIptc {
    @POST("rest/process")
    Call<IptcResponse> process(@Body IptcRequest request);
}
