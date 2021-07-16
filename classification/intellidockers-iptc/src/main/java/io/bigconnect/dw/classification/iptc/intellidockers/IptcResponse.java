package io.bigconnect.dw.classification.iptc.intellidockers;

import java.util.ArrayList;
import java.util.List;

public class IptcResponse {
    public List<IptcCategory> categories = new ArrayList<>();

    public static class IptcCategory {
        public String label;
        public Double score;
    }
}
