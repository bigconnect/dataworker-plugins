package io.bigconnect.dw.ner.intellidockers;

import java.util.ArrayList;
import java.util.List;

public class Entities {
    public List<Entity> entities = new ArrayList<>();

    public static class Entity {
        public String entity;
        public String type;
        public int count;
    }
}
