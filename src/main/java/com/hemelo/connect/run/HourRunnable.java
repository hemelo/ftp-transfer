package com.hemelo.connect.run;

import com.hemelo.connect.MainAux;
import com.hemelo.connect.constants.Dates;
import com.hemelo.connect.enums.TopicEmail;
import com.hemelo.connect.infra.Mailer;
import com.hemelo.connect.utils.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.nio.file.Files;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;

public class HourRunnable implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(HourRunnable.class);

    @Override
    public void run() {

        synchronized (MainAux.isSendingRelatorios) {
            MainAux.isSendingRelatorios.set(true);
            MainAux.isSendingRelatorios.notifyAll();
        }

        logger.debug("Executando cronjob de 1 hora");

        Duration uptime = null;

        try {
            RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
            uptime = Duration.ofMillis(runtimeMXBean.getUptime());

            if (uptime.toMinutes() < 30) {
                logger.debug("O sistema está em execução há menos de 30 minutos. Uptime: " + uptime.toMinutes() + " minutos");

                synchronized (MainAux.isSendingRelatorios) {
                    MainAux.isSendingRelatorios.set(false);
                    MainAux.isSendingRelatorios.notifyAll();
                }

                return;
            }

        } catch (Exception e) {
            logger.error("Erro ao obter o tempo de execução", e);
        }

        StringBuilder sb = MainAux.getRelatorioSistema();

        String path = MainAux.getCaminhoLogAplicacao();
        FileUtils.createDirectoryIfNotExists(path);

        File folder = new File(path);

        String nomeZip = "logs-para-email-" + Dates.UNIVERSAL_DATETIME_FORMATTER.format(LocalDateTime.now(Dates.ZONE_ID));String arquivoZip = path + File.separator + nomeZip + ".zip";

        if (StringUtils.isBlank(path) || !folder.exists()) {
            sb.append("❌ Não foi possível encontrar o diretório de logs diários").append(System.lineSeparator());
        } else {
            long size = FileUtils.getFolderSize(new File(path));
            sb.append("📊 Tamanho do diretório de logs diários: ").append(size).append(" bytes").append(System.lineSeparator());

            try  {

                FileUtils.zipFile(path, arquivoZip);

                sb.append("📂 Segue em anexo os logs compactados").append(System.lineSeparator());

                logger.info("Logs compactadas com sucesso.");
            } catch (Exception e) {
                logger.info("Erro ao criar o arquivo ZIP: " + e.getMessage());
            }
        }

        synchronized (MainAux.isSendingRelatorios) {
            MainAux.isSendingRelatorios.set(false);
            MainAux.isSendingRelatorios.notifyAll();
        }

        Thread mailThread = Mailer.getInstance().sendMail(TopicEmail.RELATORIO, sb.toString(), List.of(new File(arquivoZip)));

        try {
            mailThread.join();
        } catch (InterruptedException e) {
            logger.error("Erro ao aguardar o envio do email", e);
        }

        try {
            Files.deleteIfExists(new File(arquivoZip).toPath());
        } catch (IOException e) {
            logger.error("Erro ao deletar o arquivo ZIP", e);
        }
    }
}
