package com.hemelo.connect.run;

import com.hemelo.connect.MainAux;

public class ArquivosFtpFinderRunnable implements Runnable {

    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ArquivosFtpFinderRunnable.class);

    @Override
    public void run() {
        // Verifica se já está preparando para baixar arquivos do FTP
        // Se estiver preparando para baixar arquivos, aguarda a finalizacao
        synchronized (MainAux.isPreparingToRetrieveFilesFromFtp) {
            while (MainAux.isPreparingToRetrieveFilesFromFtp.get()) {
                try {
                    logger.debug("Aguardando a finalizacao da thread de preparar para baixar arquivos do FTP");
                    MainAux.isPreparingToRetrieveFilesFromFtp.wait();
                } catch (InterruptedException e) {
                    logger.error("Erro ao aguardar a finalizacao da thread de preparar para baixar arquivos do FTP", e);
                }
            }
        }

        // Verifica se já está baixando arquivos
        // Se estiver baixando arquivos, aguarda a finalizacao
        synchronized (MainAux.isDownloadingFilesFtp) {
            while (MainAux.isDownloadingFilesFtp.get()) {
                try {
                    logger.debug("Aguardando a finalizacao da thread de baixar arquivos do FTP");
                    MainAux.isDownloadingFilesFtp.wait();
                } catch (InterruptedException e) {
                    logger.error("Erro ao aguardar a finalizacao da thread de baixar arquivos do FTP", e);
                }
            }
        }

        // Procura por novos arquivos
        try {
            new ArquivosFtpVerifierRunnable().run();
        } catch (Exception e) {
            logger.error("Erro ao procurar novos arquivos", e);
        }
    }
}
