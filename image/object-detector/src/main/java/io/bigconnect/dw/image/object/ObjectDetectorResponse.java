package io.bigconnect.dw.image.object;

import java.util.List;
import java.util.Objects;

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

        @Override
        public int hashCode() {
            return Objects.hash(bottom_right_x, bottom_right_y, top_left_x, top_left_y);
        }
    }
}
