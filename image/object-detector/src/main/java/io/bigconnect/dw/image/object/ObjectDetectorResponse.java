package io.bigconnect.dw.image.object;

import java.util.List;

public class ObjectDetectorResponse {
    public List<ObjectDetectorItem> objects;

    public static class ObjectDetectorItem {
        public String label;
        public double score;
        public ObjectDetectorBox box;
    }

    public static class ObjectDetectorBox {
        public double bottom_right_x;
        public double bottom_right_y;
        public double top_left_x;
        public double top_left_y;
    }
}
