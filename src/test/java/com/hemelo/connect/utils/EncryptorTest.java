package com.hemelo.connect.utils;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class EncryptorTest {

    @Test
    void testEncrypt() {
        Encryptor encryptor = Encryptor.getInstance();
        String encrypted = encryptor.encrypt("123456");
        String decrypted = encryptor.decrypt(encrypted);
        assertEquals("123456", decrypted);

        //System.out.println("Encrypted: " + Encryptor.getInstance().encrypt(""));
    }
}
