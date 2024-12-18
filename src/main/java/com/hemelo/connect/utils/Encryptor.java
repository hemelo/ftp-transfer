package com.hemelo.connect.utils;

import org.jasypt.encryption.pbe.StandardPBEStringEncryptor;
import org.jasypt.iv.RandomIvGenerator;
import org.jasypt.properties.EncryptableProperties;
import org.jasypt.salt.RandomSaltGenerator;

/**
 * Classe utilitaria para criptografia de propriedades
 */
public class Encryptor {

    private StandardPBEStringEncryptor stringEncryptor;

    private static Encryptor instance;

    /**
     * Instancia e configura encryptor
     */
    private Encryptor () {
        stringEncryptor = new StandardPBEStringEncryptor();
        stringEncryptor.setPassword("123456");
        stringEncryptor.setAlgorithm("PBEWithMD5AndDES");
        stringEncryptor.setStringOutputType("base64");
        stringEncryptor.setSaltGenerator(new RandomSaltGenerator());
        stringEncryptor.setIvGenerator(new RandomIvGenerator());
    }

    public static Encryptor getInstance() {
        if (instance == null) instance = new Encryptor();
        return instance;
    }

    public String decrypt(String str) {
        return stringEncryptor.decrypt(str);
    }

    public String encrypt(String str) {
        return stringEncryptor.encrypt(str);
    }

    public EncryptableProperties getEncryptablePropertiesInstance() {
        return new EncryptableProperties(stringEncryptor);
    }
}
