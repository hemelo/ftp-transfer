package com.hemelo.connect;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class MainTest {

    @Test
    void testMain() {
        Assertions.assertTrue(Main.isProductionEnvironment);
        Assertions.assertTrue(Main.validaHashArquivoDb);
        Assertions.assertTrue(Main.enviarEmails);
    }
}
