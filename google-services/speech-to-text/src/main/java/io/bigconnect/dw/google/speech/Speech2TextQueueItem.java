package io.bigconnect.dw.google.speech;

import com.mware.core.model.longRunningProcess.LongRunningProcessQueueItemBase;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Speech2TextQueueItem extends LongRunningProcessQueueItemBase  {
    public static final String TYPE = "s2t";

    private String userId;
    private String workspaceId;
    private String[] authorizations;
    private String vertexId;

    @Override
    public String getType() {
        return TYPE;
    }
}
