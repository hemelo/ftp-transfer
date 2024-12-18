package com.hemelo.connect.run;

import com.hemelo.connect.Main;
import com.hemelo.connect.MainAux;
import com.hemelo.connect.constants.Caminhos;
import com.hemelo.connect.constants.Dates;
import com.hemelo.connect.constants.Retries;
import com.hemelo.connect.constants.Timings;
import com.hemelo.connect.dao.TransitoDao;
import com.hemelo.connect.dto.FileWrapper;
import com.hemelo.connect.enums.*;
import com.hemelo.connect.infra.FTPClient;
import com.hemelo.connect.infra.Mailer;
import com.hemelo.connect.utils.DateUtils;
import com.hemelo.connect.utils.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Optional;

/**
 * Retorna uma funcao responsavel por enviar os arquivos para o Connect
 */
public class ArquivosSenderRunnable implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(ArquivosSenderRunnable.class);

    private TransitoDao transitoDao;

    @Override
    public void run() { 
        
        boolean resultadoExclusao = false, gravouArquivosEnviadosDia = false, gravouArquivosComErroDia = false, gravouArquivosEnviadosExecucao = false, gravouArquivosComErroExecucao = false, gravouArquivosComErroDelecaoFtpDia = false, gravouArquivosComErroDelecaoFtpExecucao = false;
        IOException gravouArquivosEnviadosDiaException = null, gravouArquivosComErroDiaException = null, gravouArquivosEnviadosExecucaoException = null, gravouArquivosComErroExecucaoException = null, gravouArquivosComErroDelecaoFtpExecucaoException = null, gravouArquivosComErroDelecaoFtpDiaException = null;
        Exception exclusaoException = null;
        String diretorioEnvioConnect, detalheArquivo;
        int tentativas = 0, tentativasDelete = 0, erros = 0, enviados = 0;
        boolean resultado = false;

        if (MainAux.lockProcessamento.isLocked()) {
            logger.info("Não é possível baixar arquivos do FTP. O processo está bloqueado.");
            return;
        }

        synchronized (MainAux.isSendingFilesToConnect) {
            if (MainAux.isSendingFilesToConnect.get()) {
                logger.debug("Já está enviando arquivos para o Connect");
                return;
            }

            MainAux.isSendingFilesToConnect.set(true);
            MainAux.isSendingFilesToConnect.notifyAll();
        }

        // Se o processo de resetar dados estiver em andamento, aguarda a finalizacao
        synchronized (MainAux.isResettingData) {

            while (MainAux.isResettingData.get()) {
                try {
                    logger.debug("Aguardando a finalizacao do processo de resetar dados...");
                    MainAux.isResettingData.wait();
                } catch (InterruptedException e) {
                    logger.error("Erro ao aguardar a finalizacao da thread de resetar dados", e);
                }
            }
        }
        
        // Aguarda a finalizacao do processo de preparar para baixar arquivos do FTP
        synchronized (MainAux.isPreparingToRetrieveFilesFromFtp) {
            while (MainAux.isPreparingToRetrieveFilesFromFtp.get()) {
                try {
                    logger.debug("Aguardando a finalizacao do processo de preparar para baixar arquivos do FTP...");
                    MainAux.isPreparingToRetrieveFilesFromFtp.wait();
                } catch (InterruptedException e) {
                    logger.error("Erro ao aguardar a finalizacao da thread de preparar para baixar arquivos do FTP", e);
                }
            }
        }

        // Aguarda a finalizacao do processo de baixar arquivos do FTP
        synchronized (MainAux.isDownloadingFilesFtp) {
            while (MainAux.isDownloadingFilesFtp.get()) {
                try {
                    logger.debug("Aguardando a finalizacao do processo de baixar arquivos do FTP...");
                    MainAux.isDownloadingFilesFtp.wait();
                } catch (InterruptedException e) {
                    logger.error("Erro ao aguardar a finalizacao do processo de baixar arquivos do FTP", e);
                }
            }
        }

        // Atualiza o status
        synchronized (MainAux.status) {
            MainAux.status.set("Enviando arquivos para o Connect...");
            MainAux.status.notifyAll();
        }

        if (transitoDao == null && Main.validaHashArquivoDb) {
            transitoDao = new TransitoDao(MainAux.getDatasource());
        }

        MainAux.ultimoEnvioArquivosInstant = Instant.now();
        MainAux.idFilesSend.set(MainAux.idFilesSend.get() + 1);

        final Path caminhoArquivosComErroDelecaoDia = MainAux.getCaminhoArquivosComErroExclusaoDia();
        final Path caminhoArquivosEnviadosDia = MainAux.getCaminhoArquivosEnviadosDia();
        final Path caminhoArquivosComErroDia = MainAux.getCaminhoArquivosComFalhaEnvioDia();
        final Path caminhoArquivosComErroDelecaoExecucao = MainAux.getCaminhoArquivosComErroExclusaoExecucao();
        final Path caminhoArquivosEnviadosExecucao = MainAux.getCaminhoArquivosEnviadosExecucao();
        final Path caminhoArquivosComErroExecucao = MainAux.getCaminhoArquivosComFalhaEnvioExecucao();

        final StringBuilder arquivosEnviados = new StringBuilder();
        final StringBuilder arquivosEnviadosDetalhado = new StringBuilder();
        final StringBuilder arquivosErroEnvio = new StringBuilder();
        final StringBuilder arquivosErroEnvioDetalhado = new StringBuilder();
        final StringBuilder arquivosErroExclusao = new StringBuilder();
        final StringBuilder arquivosErroExclusaoDetalhado = new StringBuilder();

        for (FileWrapper fileBase : MainAux.arquivosParaEnviar) {

            if (fileBase.getStatusLocal() != FileStatusLocal.DISPONIVEL || !fileBase.isCredenciado() || fileBase.getStatusEnvio() == FileStatusEnvio.ENVIADO) {
                continue;
            }

            tentativasDelete = 0;
            resultadoExclusao = false;
            
            // O diretório de envio para o Connect é o mesmo do diretório onde o arquivo foi baixado
            diretorioEnvioConnect = fileBase.getParent();

            if (StringUtils.isNotBlank(diretorioEnvioConnect)) {

                // Verifica se diretório de envio para o Connect existe e cria se não existir
                FileUtils.createDirectoryIfNotExists(diretorioEnvioConnect);

                fileBase.setEnvioCaminhoBase(diretorioEnvioConnect);

                resultado = false;
                tentativas = 0;

                while (tentativas++ <= Retries.MAX_RETRY_SEND_FILE_TO_CONNECT && !resultado) {

                    try {
                        Files.copy(Path.of(fileBase.getLocalCaminhoCompleto()), Path.of(fileBase.getCaminhoEnvioCompleto()), StandardCopyOption.REPLACE_EXISTING);
                        logger.info(String.format("Arquivo \"%s\" copiado para a pasta do Connect em \"%s\"", fileBase.getLocalCaminhoCompleto(), diretorioEnvioConnect));
                        resultado = true;
                    } catch (IOException e) {
                        logger.error(String.format("Tentativa %d. Erro ao enviar o arquivo %s para o Connect", tentativas, fileBase.getLocalCaminhoBase()), e);
                    }
                }
            } else {
                logger.error(String.format("Diretório base %s não encontrado", fileBase.getParent()));
                resultado = false;
            }

            if (resultado) {
                fileBase.setStatusEnvio(FileStatusEnvio.ENVIADO);
                detalheArquivo = MainAux.getDetalhesArquivo(fileBase);
                arquivosEnviados.append(fileBase.getFtpCaminhoCompleto()).append(System.lineSeparator());
                arquivosEnviadosDetalhado.append(detalheArquivo).append(System.lineSeparator());

                try {
                    if (FileUtils.addSuffixToFile(fileBase.getLocalCaminhoCompleto(), Caminhos.SUFIXO_ARQUIVO_ENVIADO)) {
                        logger.info(String.format("Arquivo %s já existia e foi renomeado", fileBase.getLocalCaminhoCompleto() + "." + Caminhos.SUFIXO_ARQUIVO_ENVIADO));
                    }

                } catch (IOException e) {
                    logger.error(String.format("Erro ao tentar renomear o arquivo %s para %s.%s", fileBase.getLocalCaminhoCompleto(), fileBase.getLocalCaminhoCompleto(), Caminhos.SUFIXO_ARQUIVO_ENVIADO), e);
                }

                // Após baixá-lo, exclui o arquivo do FTP

                if (Main.isProductionEnvironment) {

                    tentativasDelete = 0;

                    while (tentativasDelete++ <= Retries.MAX_RETRY_DELETE_FILE) {
                        try {
                            FTPClient.getInstance().deleteFile(fileBase.getFtpCaminhoCompleto());
                            resultadoExclusao = true;
                            break;
                        } catch (Exception e) {
                            logger.error(String.format("Tentativa %d. Erro ao tentar deletar o arquivo %s do FTP", tentativasDelete, fileBase.getFtpCaminhoCompleto()), e);
                            exclusaoException = e;
                        }
                    }
                } else {
                    resultadoExclusao = true;
                }

                if (!resultadoExclusao) {
                    fileBase.setAdicional("Erro ao excluir arquivo do FTP: " + Optional.ofNullable(exclusaoException).map(Exception::getMessage).orElse("Erro desconhecido"));
                    detalheArquivo = MainAux.getDetalhesArquivo(fileBase);
                    arquivosErroExclusao.append(fileBase.getFtpCaminhoCompleto()).append(System.lineSeparator());
                    arquivosErroExclusaoDetalhado.append(detalheArquivo).append(System.lineSeparator());
                } else {
                    fileBase.setStatusFtp(FileStatusRemoto.DELETADO);
                }

                if (Main.validaHashArquivoDb) {

                    try {
                        transitoDao.atualizaStatusArquivo(fileBase, FileStatusDatabase.ENVIADO);
                    } catch (Exception e) {
                        logger.error("Erro ao atualizar o status do arquivo no banco de dados", e);
                    }
                }

                enviados++;

            } else {
                fileBase.setStatusEnvio(FileStatusEnvio.ERRO_ENVIO);
                detalheArquivo = MainAux.getDetalhesArquivo(fileBase);
                arquivosErroEnvio.append(fileBase.getFtpCaminhoCompleto()).append(System.lineSeparator());
                arquivosErroEnvioDetalhado.append(detalheArquivo).append(System.lineSeparator());

                erros++;
            }
        }


        // Aguarda a finalizacao do processo de limpeza de logs
        synchronized (MainAux.isCleaningLogs) {
            while (MainAux.isCleaningLogs.get()) {
                try {
                    logger.debug("Aguardando a finalizacao do processo de limpeza de logs...");
                    MainAux.isCleaningLogs.wait();
                } catch (InterruptedException e) {
                    logger.error("Erro ao aguardar a finalizacao da thread de limpeza de logs", e);
                }
            }
        }

        // Grava os arquivos enviados e perdidos em arquivos de log

        tentativas = 0;

        while (tentativas++ <= Retries.MAX_RETRY_GRAVAR_LOG) {

            if (!gravouArquivosComErroDelecaoFtpDia) {
                try {
                    if (!arquivosErroExclusao.isEmpty()) {
                        Files.writeString(caminhoArquivosComErroDelecaoDia, arquivosErroExclusaoDetalhado.toString(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                    }

                    gravouArquivosComErroDelecaoFtpDia = true;
                } catch (IOException e) {
                    logger.error("Erro ao gravar arquivos com erro de exclusao do dia em " + caminhoArquivosComErroDelecaoDia, e);
                    gravouArquivosComErroDelecaoFtpExecucaoException = e;
                }
            }

            if (!gravouArquivosEnviadosDia) {
                try {
                    if (!arquivosEnviados.isEmpty()) {
                        Files.writeString(caminhoArquivosEnviadosDia, arquivosEnviadosDetalhado.toString(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                    }

                    gravouArquivosEnviadosDia = true;
                } catch (IOException e) {
                    logger.error("Erro ao gravar arquivos enviados do dia em " + caminhoArquivosEnviadosDia, e);
                    gravouArquivosEnviadosDiaException = e;
                }
            }

            if (!gravouArquivosComErroDia) {
                try {
                    if (!arquivosErroEnvio.isEmpty()) {
                        Files.writeString(caminhoArquivosComErroDia, arquivosErroEnvioDetalhado.toString(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                    }

                    gravouArquivosComErroDia = true;
                } catch (IOException e) {
                    logger.error("Erro ao gravar arquivos com erro do dia em " + caminhoArquivosComErroDia, e);
                    gravouArquivosComErroDiaException = e;
                }
            }

            if (!gravouArquivosComErroDelecaoFtpExecucao) {
                try {
                    if (!arquivosErroExclusao.isEmpty()) {
                        Files.writeString(caminhoArquivosComErroDelecaoExecucao, arquivosErroExclusaoDetalhado.toString(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                    }

                    gravouArquivosComErroDelecaoFtpExecucao = true;
                } catch (IOException e) {
                    logger.error("Erro ao gravar arquivos com erro de exclusao da execucao em " + caminhoArquivosComErroDelecaoExecucao, e);
                    gravouArquivosComErroDelecaoFtpExecucaoException = e;
                }
            }

            if (!gravouArquivosEnviadosExecucao) {
                try {
                    if (!arquivosEnviados.isEmpty()) {
                        Files.writeString(caminhoArquivosEnviadosExecucao, arquivosEnviadosDetalhado.toString(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                    }

                    gravouArquivosEnviadosExecucao = true;
                } catch (IOException e) {
                    logger.error("Erro ao gravar arquivos enviados da execucao em " + caminhoArquivosEnviadosExecucao, e);
                    gravouArquivosEnviadosExecucaoException = e;
                }
            }

            if (!gravouArquivosComErroExecucao) {
                try {
                    if (!arquivosErroEnvio.isEmpty()) {
                        Files.writeString(caminhoArquivosComErroExecucao, arquivosErroEnvioDetalhado.toString(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                    }

                    gravouArquivosComErroExecucao = true;
                } catch (IOException e) {
                    logger.error("Erro ao gravar arquivos com erro da execucao em " + caminhoArquivosComErroExecucao, e);
                    gravouArquivosComErroExecucaoException = e;
                }
            }

            // Se todos os arquivos foram gravados, sai do loop
            if (gravouArquivosEnviadosDia && gravouArquivosComErroDia && gravouArquivosEnviadosExecucao && gravouArquivosComErroExecucao && gravouArquivosComErroDelecaoFtpDia && gravouArquivosComErroDelecaoFtpExecucao) {
                break;
            }

            // Aguarda um periodo para tentar gravar os arquivos que não foram gravados novamente
            try {
                Thread.sleep(Timings.INTERVALO_ERRO_IO_RETRY);
            } catch (InterruptedException e) {
                logger.error("Erro ao aguardar 1 segundo para gravar arquivos enviados e perdidos", e);
            }
        }

        try {
            // Se não foi possível gravar os arquivos enviados e perdidos, notifica o erro via email
            if (!gravouArquivosEnviadosDia || !gravouArquivosComErroDia || !gravouArquivosEnviadosExecucao || !gravouArquivosComErroExecucao || !gravouArquivosComErroDelecaoFtpDia || !gravouArquivosComErroDelecaoFtpExecucao ) {
                logger.error("Erro ao gravar log de arquivos enviados e com erro");

                StringBuilder body = new StringBuilder();

                body.append(DateUtils.getCumprimento()).append(",").append(System.lineSeparator()).append(System.lineSeparator());
                body.append(" houve um erro ao gravar log de arquivos enviados e com erro").append(System.lineSeparator()).append(System.lineSeparator());

                if (!gravouArquivosEnviadosDia) {
                    body.append("❌ Erro ao atualizar arquivos enviados do dia em ").append(caminhoArquivosEnviadosDia).append(System.lineSeparator());
                    body.append("Exception: ").append(gravouArquivosEnviadosDiaException).append(System.lineSeparator()).append(System.lineSeparator());
                }

                if (!gravouArquivosComErroDia) {
                    body.append("❌ Erro ao atualizar arquivos com erro do dia em ").append(caminhoArquivosComErroDia).append(System.lineSeparator());
                    body.append("Exception: ").append(gravouArquivosComErroDiaException).append(System.lineSeparator()).append(System.lineSeparator());
                }

                if (!gravouArquivosEnviadosExecucao) {
                    body.append("❌ Erro ao gravar arquivos enviados da execução " + MainAux.idFilesSend.get() + " em ").append(caminhoArquivosEnviadosExecucao).append(System.lineSeparator());
                    body.append("Exception: ").append(gravouArquivosEnviadosExecucaoException).append(System.lineSeparator()).append(System.lineSeparator());
                }

                if (!gravouArquivosComErroExecucao) {
                    body.append("❌ Erro ao gravar arquivos com erro da execução" + MainAux.idFilesSend.get() + " em ").append(caminhoArquivosComErroExecucao).append(System.lineSeparator());
                    body.append("Exception: ").append(gravouArquivosComErroExecucaoException).append(System.lineSeparator()).append(System.lineSeparator());
                }

                if (!gravouArquivosComErroDelecaoFtpDia) {
                    body.append("❌ Erro ao gravar arquivos com erro de exclusao do dia em ").append(caminhoArquivosComErroDelecaoDia).append(System.lineSeparator());
                    body.append("Exception: ").append(gravouArquivosComErroDelecaoFtpDiaException).append(System.lineSeparator()).append(System.lineSeparator());
                }

                if (!gravouArquivosComErroDelecaoFtpExecucao) {
                    body.append("❌ Erro ao gravar arquivos com erro de exclusao da execucao em ").append(caminhoArquivosComErroDelecaoExecucao).append(System.lineSeparator());
                    body.append("Exception: ").append(gravouArquivosComErroDelecaoFtpExecucaoException).append(System.lineSeparator()).append(System.lineSeparator());
                }

                Mailer.getInstance().sendMail(TopicEmail.ALERTA_GRAVACAO_LOG_ARQUIVOS, body.toString());
            }
        } catch (Exception e) {
            logger.error("Erro ao enviar email de alerta de erro ao gravar log de arquivos enviados e com erro", e);
        }

        try {
            if (!arquivosEnviados.isEmpty() || !arquivosErroEnvio.isEmpty()) {

                StringBuilder body = new StringBuilder();
                body.append(DateUtils.getCumprimento()).append(",").append(System.lineSeparator()).append(System.lineSeparator());

                body.append("📝 Segue relatório detalhado do envio de arquivos em 📅 ").append(Dates.BRAZILIAN_DATE_TIME_FORMATTER.format(LocalDateTime.ofInstant(MainAux.ultimoEnvioArquivosInstant, Dates.ZONE_ID))).append("").append(System.lineSeparator()).append(System.lineSeparator());

                if (!arquivosEnviados.isEmpty()) {
                    body.append("✅ Arquivos enviados com sucesso:").append(System.lineSeparator()).append(System.lineSeparator());
                    body.append(arquivosEnviados).append(System.lineSeparator());
                    body.append("▶️ Para mais detalhes, verifique o(s) arquivo(s) em ").append(caminhoArquivosEnviadosExecucao).append(System.lineSeparator());
                }

                if (!arquivosErroEnvio.isEmpty()) {

                    if (!arquivosEnviados.isEmpty()) {
                        body.append(System.lineSeparator()).append(System.lineSeparator());
                    }

                    body.append("❌ Arquivos que deram erro ao enviar:").append(System.lineSeparator());
                    body.append(arquivosErroEnvio).append(System.lineSeparator()).append(System.lineSeparator());
                    body.append("▶️ Para mais detalhes, verifique o(s) motivo(s) do(s) erro(s) em ").append(caminhoArquivosComErroExecucao).append(System.lineSeparator());
                }

                if (!arquivosErroExclusao.isEmpty()) {

                    if (!arquivosEnviados.isEmpty() || !arquivosErroEnvio.isEmpty()) {
                        body.append(System.lineSeparator()).append(System.lineSeparator());
                    }

                    body.append("❌ Arquivos que deram erro ao excluir do FTP:").append(System.lineSeparator());
                    body.append(arquivosErroExclusao).append(System.lineSeparator()).append(System.lineSeparator());
                    body.append("▶️ Para mais detalhes, verifique o(s) motivo(s) do(s) erro(s) em ").append(caminhoArquivosComErroDelecaoExecucao).append(System.lineSeparator());
                }

                Mailer.getInstance().sendMail(arquivosEnviados.isEmpty() ? TopicEmail.ARQUIVO_NAO_ENVIADO_ERRO_PARA_CONNECT : TopicEmail.ARQUIVO_ENVIADO_PARA_CONNECT, body.toString());
            }
        } catch (Exception e) {
            logger.error("Erro ao enviar email de arquivo de envio de arquivos para o Connect", e);
        }

        if (enviados > 0) {
            logger.info(String.format("Foram enviados %d arquivos para o Connect", enviados));
        }

        if (erros > 0) {
            logger.error(String.format("Não foi possível enviar %d arquivos para o Connect", erros));
        }

        if (enviados == 0 && erros == 0) {
            logger.info("Não há arquivos para enviar para o Connect");
        }

        // Sinaliza que o envio para o Connect foi finalizado
        synchronized (MainAux.isSendingFilesToConnect) {
            MainAux.isSendingFilesToConnect.set(false);
            MainAux.isSendingFilesToConnect.notifyAll();
        }

        // Atualiza o status
        synchronized (MainAux.status) {
            MainAux.status.set("");
            MainAux.status.notifyAll();
        }
    }
}
