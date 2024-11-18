package com.hemelo.connect.run;

import com.hemelo.connect.MainAux;
import com.hemelo.connect.constants.Retries;
import com.hemelo.connect.enums.FileStatusEnvio;
import com.hemelo.connect.enums.FileStatusRemoto;
import com.hemelo.connect.utils.DateUtils;
import com.hemelo.connect.utils.ProcessaUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Retorna uma funcao que deve ser executada a cada minuto
 */
public class MinuteRunnable implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(MinuteRunnable.class);
    
    @Override
    public void run() {

        logger.debug("Executando cronjob de 1 minuto");

        this.limparLogs();

        // Verifica se está virando o dia e locka o inicio de novas operações da aplicação
        if (DateUtils.isSwitchingDay()) {
            if (!MainAux.lockProcessamento.isLocked()) {
                logger.debug("Está virando o dia. Bloqueando o processamento...");
                MainAux.lockProcessamento.lock();
            }
        } else {
            if (MainAux.lockProcessamento.isLocked()) {
                try {
                    logger.debug("Liberando o processamento novamente...");
                    MainAux.lockProcessamento.unlock();
                } catch (IllegalMonitorStateException e) {
                    logger.error("Erro ao liberar o lock de processamento", e);
                }
            }
        }

        if (DateUtils.isMidnight()) { // Se for meia noite, roda uma thread para resetar os dados

            logger.debug("É meia noite. Executando thread para reset dos dados...");

            ProcessaUtils.createThread("Reset Dados", () -> {
                synchronized (MainAux.isResettingData) {
                    if (MainAux.isResettingData.get()) {
                        logger.debug("Já está resetando os dados");
                        return;
                    }

                    MainAux.isResettingData.set(true);
                    MainAux.isResettingData.notifyAll();
                }

                synchronized (MainAux.isDownloadingFilesFtp) {
                    while (MainAux.isDownloadingFilesFtp.get()) {
                        try {
                            logger.debug("Aguardando finalizar o processo de baixar arquivos do FTP...");
                            MainAux.isDownloadingFilesFtp.wait();
                        } catch (InterruptedException e) {
                            logger.error("Erro ao aguardar a finalizacao do processo de baixar arquivos do FTP", e);
                        }
                    }
                }

                synchronized (MainAux.isSendingFilesToConnect) {
                    while (MainAux.isSendingFilesToConnect.get()) {
                        try {
                            logger.debug("Aguardando finalizar o processo de enviar arquivos para o Connect...");
                            MainAux.isSendingFilesToConnect.wait();
                        } catch (InterruptedException e) {
                            logger.error("Erro ao aguardar a finalizacao do processo de enviar arquivos para o Connect", e);
                        }
                    }
                }

                synchronized (MainAux.status) {
                    MainAux.status.set("Resetando dados...");
                    MainAux.status.notifyAll();
                }

                // Reseta os IDs
                MainAux.idFilesSend.set(0);
                MainAux.idFilesRetrieval.set(0);

                // Reseta os arquivos
                // Remove arquivos que não são arquivos, que já foram enviados, ou que não foram encontrados no FTP e houve uma tentativa de download
                MainAux.arquivosParaEnviar.removeIf(
                        a -> !a.isCredenciado() ||
                                a.getStatusEnvio() == FileStatusEnvio.ENVIADO ||
                                (a.getStatusFtp() == FileStatusRemoto.REFERENCIA_PERDIDA && a.getFindedAt().isBefore(Instant.now().minus(1, ChronoUnit.DAYS))));


                synchronized (MainAux.status) {
                    MainAux.status.set("");
                    MainAux.status.notifyAll();
                }

                synchronized (MainAux.isResettingData) {
                    MainAux.isResettingData.set(false);
                    MainAux.isResettingData.notifyAll();
                }
            });
        }
    }

    private void limparLogs() {
        ProcessaUtils.createThread("Limpa Logs Vazios", () -> {

            if (MainAux.isCleaningLogs.get()) return;

            // Aguarda a finalizacao da thread de download de arquivos
            synchronized (MainAux.isDownloadingFilesFtp) {
                while (MainAux.isDownloadingFilesFtp.get()) {
                    try {
                        MainAux.isDownloadingFilesFtp.wait();
                    } catch (InterruptedException e) {
                        logger.error("Erro ao aguardar a finalizacao da thread de download de arquivos", e);
                    }
                }
            }

            // Aguarda a finalizacao da thread de envio de arquivos
            synchronized (MainAux.isSendingFilesToConnect) {
                while (MainAux.isSendingFilesToConnect.get()) {
                    try {
                        MainAux.isSendingFilesToConnect.wait();
                    } catch (InterruptedException e) {
                        logger.error("Erro ao aguardar a finalizacao da thread de envio de arquivos", e);
                    }
                }
            }

            // Verifica se já está limpando os arquivos de logs
            synchronized (MainAux.isCleaningLogs) {
                MainAux.isCleaningLogs.set(true);
                MainAux.isCleaningLogs.notifyAll();
            }

            synchronized (MainAux.status) {
                MainAux.status.set("Limpando arquivos de log...");
                MainAux.status.notifyAll();
            }

            try {
                String logDirectory = MainAux.getCaminhoArquivosLogDiarios();
                java.util.List<File> files = Arrays.asList(Objects.requireNonNullElse(new File(logDirectory).listFiles(), new File[0]));

                List<String> linhas;
                String error;
                int tentativas;
                String header = MainAux.getHeaderLogFile().replace(System.lineSeparator(), "");

                for (File file : files) {
                    if (file.isDirectory()) continue;

                    tentativas = 0;
                    error = null;

                    try {
                        linhas = Files.readAllLines(file.toPath());

                        if (linhas.size() > 1 && !linhas.get(1).isBlank()) continue;

                        if (linhas.isEmpty() || linhas.get(0).isBlank()) {
                            error = "Arquivo de log vazio " + file.getName();
                        }

                        if (linhas.get(0).equalsIgnoreCase(header)) {
                            error = "Arquivo de log com apenas o header " + file.getName();
                        }

                        if (StringUtils.isNotBlank(error)) {

                            while (tentativas++ <= Retries.MAX_RETRY_DELETE_FILE) {
                                try {
                                    Files.deleteIfExists(file.toPath());
                                    logger.trace("Arquivo de log deletado, motivo: " + error);
                                    break;
                                } catch (IOException ex) {
                                    logger.trace(String.format("Tentativa %d. Erro ao deletar o arquivo de log %s ", tentativas, file.getName()));
                                }
                            }
                        }
                    } catch (IOException ex) {
                        logger.trace("Erro ao ler as linhas do arquivo " + file.getName());
                    }
                }

            } finally {
                synchronized (MainAux.isCleaningLogs) {
                    MainAux.isCleaningLogs.set(false);
                    MainAux.isCleaningLogs.notifyAll();
                }
            }

        });
    }

}
