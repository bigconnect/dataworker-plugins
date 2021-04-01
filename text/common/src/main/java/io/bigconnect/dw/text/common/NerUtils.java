package io.bigconnect.dw.text.common;

import com.mware.core.model.termMention.TermMentionRepository;
import com.mware.core.model.termMention.TermMentionUtils;
import com.mware.ge.Authorizations;
import com.mware.ge.Graph;
import com.mware.ge.Vertex;

public class NerUtils {
    public static synchronized  void removeTermMentions(
            Vertex outVertex,
            TermMentionRepository termMentionRepository,
            TermMentionUtils termMentionUtils,
            Graph graph,
            Authorizations authorizations
    ) {
        // delete existing term mentions
        termMentionRepository.deleteTermMentions(outVertex.getId(), authorizations);
        termMentionUtils.removeHasDetectedEntityRelations(outVertex);
        graph.flush();
    }
}
