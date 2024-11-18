package com.hemelo.connect.run;

import com.hemelo.connect.MainAux;
import com.hemelo.connect.constants.Caminhos;
import com.hemelo.connect.constants.Sizes;
import com.hemelo.connect.utils.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;

/**
 * Retorna uma funcao que deve ser executada diariamente
 */
public class DailyRunnable implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(DailyRunnable.class);

    @Override
    public void run() {
        logger.debug("Executando cronjob diária");

        List<File> folders = List.of(new File(MainAux.getCaminhoArquivosLog()), new File(Caminhos.DIRETORIO_ARQUIVOS_LOCAL));

        for (File folder : folders) {
            if (folder.exists() && folder.isDirectory()) {
                long size = FileUtils.getFolderSize(folder);

                if (size > Sizes.UM_GIGABYTE) {
                    //FileUtils.compressOldSubfolders(folder, Duration.of(5, ChronoUnit.DAYS));
                }
            }
        }
    }
}
