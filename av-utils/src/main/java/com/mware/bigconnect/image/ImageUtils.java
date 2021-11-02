package com.mware.bigconnect.image;

import com.mware.ge.collection.Pair;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.IOException;

public class ImageUtils {
    public static Pair<Integer, Integer> getImageSize(byte[] data) {
        try {
            BufferedImage img = ImageIO.read(new ByteArrayInputStream(data));
            return Pair.of(img.getWidth(), img.getHeight());
        } catch (IOException e) {
            e.printStackTrace();
        }

        return Pair.empty();
    }
}
