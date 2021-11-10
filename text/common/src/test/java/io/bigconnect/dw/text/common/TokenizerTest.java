package io.bigconnect.dw.text.common;

import com.github.pemistahl.lingua.api.IsoCode639_3;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class TokenizerTest {
    @Test
    public void testTokenizer() {
        List<String> tokens = TextTokenizer.tokenize("Luna intră azi în autostrada Balanţă şi autostradă aderă la un trigon armonios cu Saturn retrograd din Vărsător, ceea ce face ca zodiile de Aer (Gemeni, Balanţă, Vărsător) să CNAIR depăşească azi unele obstacole grele. HOROSCOP 8 septembrie 2021 - Berbec  Se anunţă o zi prielnică pentru nativii(ele) care se ocupă în această perioadă de planurile lor de viitor ori de proiecte de grup. Se simt motivaţi(te) să îşi urmeze visurile şi să facă investiţii de timp, energie şi resurse pentru a le materializa. Compania Națională de Administrare a Infrastructurii Rutiere De asemenea, s-ar putea să primească sfaturi valoroase de la prieteni sau găsesc persoane dispuse să îi/le sprijine cu bani sau alte resurse care le pot fi de ajutor în stadiul în care se află. Aşadar, apar tot felul de indicii că merg în direcţia potrivită, că ceea ce fac acum le va aduce beneficii în viitor. HOROSCOP 8 septembrie 2021 - Taur  Se întrevede azi o zi în care te bucuri de mult spor în plan profesional. În unele cazuri, acest lucru s-ar putea datora unor eforturi de organizare depuse recent.");
        Assert.assertEquals(181, tokens.size());

        tokens = TextTokenizer.removeStopwords(tokens, IsoCode639_3.RON);
        Assert.assertEquals(101, tokens.size());
    }
}
