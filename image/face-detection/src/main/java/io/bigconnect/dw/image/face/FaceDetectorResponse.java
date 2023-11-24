package io.bigconnect.dw.image.face;

import java.util.ArrayList;
import java.util.List;

public class FaceDetectorResponse {
    public List<FaceDetectorFace> faces = new ArrayList<>();

    public static class FaceDetectorFace {
        public int age;
        public double score;
        public String sex;
        public FaceDetectorBox box;
    }

    public static class FaceDetectorBox {
        public float x1;
        public float y1;
        public float x2;
        public float y2;
    }
}
