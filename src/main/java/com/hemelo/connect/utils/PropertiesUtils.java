package com.hemelo.connect.utils;

import com.hemelo.connect.Main;

import java.util.Optional;
import java.util.Properties;

/**
 * Classe utilitaria para leitura de arquivos de propriedades
 */
public class PropertiesUtils {

    /**
     * Retorna um objeto Properties a partir de um arquivo de propriedades
     * @param caminho caminho do arquivo de propriedades
     * @return
     */
    public static Optional<Properties> getProperties(String caminho) {

        try {
            Properties prop = Encryptor.getInstance().getEncryptablePropertiesInstance();
            prop.load(Main.class.getClassLoader().getResourceAsStream(caminho));
            return Optional.of(prop);
        }
        catch (Exception ex) {

            return Optional.empty();
        }
    }
}
