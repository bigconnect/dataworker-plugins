package io.bigconnect.dw.text.common;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class NerUtilsTest {
    String TEXT = "Directorul Termoenergetica, Claudiu Crețu, spune că ar plăti toate datoriile dacă ar avea bani: ”Când plătește Primăria subvenția, plătim și noi”.\n" +
            "\n" +
            "Primăria Capitalei are o datorie de peste 200 de milioane lei la Termoenergetica, reprezentând plata subvenției asumate pentru populație pentru ultimele luni, mai-septembrie.\n" +
            "\n" +
            "În București, doar 10-12% din rețeaua principală de termoficare este modernizată, iar pierderile sunt colosale, undeva la 2.200.000 l/h în 2020.\n" +
            "\n" +
            "Potrivit informațiilor făcute publice de compania Termoenergetica, în cei 4 ani de mandat ai Gabrielei Firea s-au modernizat peste 200 de km de rețea de termoficare, din care doar puțin peste 30 km de rețea primară.\n" +
            "\n" +
            "Într-un interviu acordat HotNews.ro la finalul lunii septembrie, Nicușor Dan declara că în 2021 au fost modernizați 15 km de rețea primară.";

    @Test
    public void testExtractParagraphs() {
        List<TextSpan> paragraphs = NerUtils.getParagraphs(TEXT);
        Assert.assertEquals(5, paragraphs.size());
    }

}
