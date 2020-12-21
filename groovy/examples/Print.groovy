import com.mware.core.ingest.graphProperty.GraphPropertyWorkData
import com.mware.ge.Element
import com.mware.ge.Property

public void execute(InputStream inputStream, GraphPropertyWorkData data) throws Exception {
    println "execute: " + data
}

public boolean isHandled(Element element, Property property) {
    println "isHandled: " + element + ", " + property
    return true;
}
