package com.hemelo.connect.utils;

/**
 * Classe responsável por filtrar os arquivos que devem ser processados e enviados para o Connect
 */
public abstract class FiltrarArquivo {

    /**
     * Responsável por validar se o arquivo é válido que deve ser processado e enviado para o Connect
     * @param filename
     * @param parentDirectory
     *
     * @return
     */
    public static Boolean validarCredenciado(String filename, String parentDirectory) {
        return true;
    }
}
