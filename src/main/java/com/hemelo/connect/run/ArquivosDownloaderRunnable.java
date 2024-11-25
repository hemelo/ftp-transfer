package com.hemelo.connect.run;

import com.hemelo.connect.Main;
import com.hemelo.connect.MainAux;
import com.hemelo.connect.constants.Caminhos;
import com.hemelo.connect.constants.Dates;
import com.hemelo.connect.constants.Retries;
import com.hemelo.connect.constants.Timings;
import com.hemelo.connect.dao.TransitoDao;
import com.hemelo.connect.dto.FileWrapper;
import com.hemelo.connect.enums.FileStatusEnvio;
import com.hemelo.connect.enums.FileStatusLocal;
import com.hemelo.connect.enums.FileStatusRemoto;
import com.hemelo.connect.enums.TopicEmail;
import com.hemelo.connect.exception.ArquivoStatusDbInvalidoException;
import com.hemelo.connect.infra.FTPClient;
import com.hemelo.connect.infra.Mailer;
import com.hemelo.connect.utils.DateUtils;
import com.hemelo.connect.utils.FileUtils;
import com.hemelo.connect.utils.ProcessaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.*;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;

import static com.hemelo.connect.constants.Timings.INTERVALO_PESQUISA_ARQUIVOS_BEFORE_RETRIEVAL_MS;

/**
 * Retorna uma funcao que baixa os arquivos do FTP
 */
public class ArquivosDownloaderRunnable implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(ArquivosDownloaderRunnable.class);

    private TransitoDao transitoDao;

    private boolean deveExecutar() {

        LocalTime now = LocalTime.now(Dates.ZONE_ID);

        if (MainAux.ultimoDownloadArquivosInstant != null) {
            LocalTime  ultimoDownload = LocalTime.ofInstant(MainAux.ultimoDownloadArquivosInstant, Dates.ZONE_ID);
            long diff = Duration.between(ultimoDownload, now).toMinutes();

            if (diff < Timings.INTERVALO_MINIMO_ENTRE_DOWNLOADS_MINUTOS) {
                return false;
            }
        }

        // Verifica se pode
        return (Timings.HORARIOS_DOWNLOAD_ARQUIVOS_FTP.stream().anyMatch(h -> h.getHour() == now.getHour() && h.getMinute() == now.getMinute()));
    }

    @Override
    public void run() {

        if (!deveExecutar()) return;

        // Variáveis
        Thread downloadWaitThread = null;
        String detalheArquivo, error = "";
        OutputStream outputStream;
        InputStream inputStream;
        List<Path> relatedFiles, relatedFilesEnviados;
        int tentativas = 0, tentativasDelete = 0;
        boolean resultado = false, resultadoTemporario = false, gravarLog = true;
        boolean gravouArquivosBaixadosDia = false, gravouArquivosPerdidosDia = false, gravouArquivosBaixadosExecucao = false, gravouArquivosPerdidosExecucao = false, gravouArquivosQueDeramErroDia = false, gravouArquivosQueDeramErroExecucao = false;
        IOException gravouArquivosEnviadosDiaException = null, gravouArquivosPerdidosDiaException = null, gravouArquivosEnviadosExecucaoException = null, gravouArquivosPerdidosExecucaoException = null, gravouArquivosQueDeramErroDiaException = null, gravouArquivosQueDeramErroExecucaoException = null;

        try {

            if (MainAux.lockProcessamento.isLocked()) {
                logger.info("Não é possível baixar arquivos do FTP. O processo está bloqueado.");
                return;
            }

            // Verifica se já está preparando para pegar arquivos do FTP
            synchronized (MainAux.isPreparingToRetrieveFilesFromFtp) {
                if (MainAux.isPreparingToRetrieveFilesFromFtp.get()) {
                    logger.debug("Já está preparando para pegar arquivos do FTP e salvar localmente");
                    return;
                }

                MainAux.isPreparingToRetrieveFilesFromFtp.set(true);
                MainAux.isPreparingToRetrieveFilesFromFtp.notifyAll();
            }

            // Se o processo de resetar dados estiver em andamento, aguarda a finalizacao
            synchronized (MainAux.isResettingData) {

                while (MainAux.isResettingData.get()) {
                    try {
                        logger.debug("Aguardando finalizacao do processo de resetar dados para baixar arquivos do FTP");
                        MainAux.isResettingData.wait();
                    } catch (InterruptedException e) {
                        logger.error("Erro ao aguardar a finalizacao da thread de resetar dados", e);
                    }
                }
            }

            // Se o processo de procurar arquivos estiver em andamento, aguarda a finalizacao
            synchronized (MainAux.isProcurandoArquivos) {

                while (MainAux.isProcurandoArquivos.get()) {
                    try {
                        logger.debug("Aguardando finalizacao do processo de procurar arquivos para baixar arquivos do FTP");
                        MainAux.isProcurandoArquivos.wait();
                    } catch (InterruptedException e) {
                        logger.error("Erro ao aguardar a finalizacao da thread de procurar arquivos", e);
                    }
                }
            }

            if (transitoDao == null && Main.validaHashArquivoDb) {
                transitoDao = new TransitoDao(MainAux.getDatasource());
            }

            final String lockFileName = createLockFileName();
            gravarArquivoLock(lockFileName);

            // Se já estiver pegando os arquivos, aguarda a finalizacao
            synchronized (MainAux.isDownloadingFilesFtp) {
                while (MainAux.isDownloadingFilesFtp.get()) {
                    try {
                        logger.debug("Aguardando finalizacao do processo de baixar arquivos do FTP");
                        MainAux.isDownloadingFilesFtp.wait();
                    } catch (InterruptedException e) {
                        logger.error("Erro ao aguardar a finalizacao do processo de baixar arquivos do FTP", e);
                    }
                }
            }

            // Atualiza o status
            synchronized (MainAux.status) {
                MainAux.status.set("Preparando para baixar arquivos do FTP...");
                MainAux.status.notifyAll();
            }

            // Se deve procurar por novos arquivos, executa a funcao de procurar novos arquivos
            // Por mais confuso que pareca, a funcao de procurar novos arquivos é executada em loop para evitar que um arquivo seja enviado pela metade
            // Se um arquivo estiver em transferencia, é necessario procurar por novos arquivos enquanto o arquivo estiver em transferencia
            if (devePesquisarPeloMenosUmaVez()) {

                logger.trace("Deve procurar por novos arquivos pelo menos uma vez");

                int idArquivosFinderThread = 0;

                // Verifica novos arquivos
                new ArquivosFtpVerifierRunnable().run();

                if (isObrigatorioPesquisarNovamente()) {
                    logger.debug("É obrigatório procurar por novos arquivos, porque tem algum arquivo em transferência");
                }

                // Se é OBRIGATORIO continuar procurando por novos arquivos, executa a funcao de procurar novos arquivos em loop novamente
                while (isObrigatorioPesquisarNovamente()) {

                    // Aguarda um tempo para verificar se deve procurar novos arquivos novamente
                    try {
                        logger.debug(String.format("Aguardando %s milissegundos para procurar novos arquivos", INTERVALO_PESQUISA_ARQUIVOS_BEFORE_RETRIEVAL_MS));
                        Thread.sleep(INTERVALO_PESQUISA_ARQUIVOS_BEFORE_RETRIEVAL_MS);
                    } catch (InterruptedException e) {
                        logger.error("Erro ao aguardar " + INTERVALO_PESQUISA_ARQUIVOS_BEFORE_RETRIEVAL_MS + " milissegundos para procurar novos arquivos", e);
                    }

                    if (!isObrigatorioPesquisarNovamente()) break;

                    // Verifica novos arquivos
                    ProcessaUtils.createThread(String.format("Arquivos Finder for Download %02d", ++idArquivosFinderThread), new ArquivosFtpVerifierRunnable());

                    synchronized (MainAux.isProcurandoArquivos) {
                        // Novamente, se o processo de procurar arquivos estiver em andamento, aguarda a finalizacao
                        while (MainAux.isProcurandoArquivos.get()) {
                            try {
                                logger.debug("Aguardando finalizacao do processo de procurar arquivos para baixar arquivos do FTP");
                                MainAux.isProcurandoArquivos.wait();
                            } catch (InterruptedException e) {
                                logger.error("Erro ao aguardar a finalização da thread de procurar arquivos", e);
                            }
                        }
                    }

                    logger.trace("Necessário pesquisar novamente: " + isObrigatorioPesquisarNovamente());
                }
            }

            MainAux.ultimoDownloadArquivosInstant = Instant.now();
            MainAux.idFilesRetrieval.set(MainAux.idFilesRetrieval.get() + 1);

            final Path caminhoLogArquivosBaixadosDia = MainAux.getCaminhoArquivosBaixadosDia();
            final Path caminhoLogArquivosPerdidosDia = MainAux.getCaminhoArquivosPerdidosDia();
            final Path caminhoLogArquivosComErroDia = MainAux.getCaminhoArquivosComFalhaDownloadDia();
            final Path caminhoLogArquivosBaixadosExecucao = MainAux.getCaminhoArquivosBaixadosExecucao();
            final Path caminhoLogArquivosPerdidosExecucao = MainAux.getCaminhoArquivosPerdidosExecucao();
            final Path caminhoLogArquivosComErroExecucao = MainAux.getCaminhoArquivosComFalhaDownloadExecucao();

            final StringBuilder arquivosBaixados = new StringBuilder();
            final StringBuilder arquivosPerdidos = new StringBuilder();
            final StringBuilder arquivosBaixadosDetalhado = new StringBuilder();
            final StringBuilder arquivosPerdidosDetalhado = new StringBuilder();
            final StringBuilder arquivosQueDeramErro = new StringBuilder();
            final StringBuilder arquivosQueDeramErroDetalhado = new StringBuilder();

            // Notifica que está baixando arquivos do FTP
            synchronized (MainAux.isDownloadingFilesFtp) {
                MainAux.isDownloadingFilesFtp.set(true);
                MainAux.isDownloadingFilesFtp.notifyAll();
            }

            // Notifica que a preparacao para baixar arquivos do FTP foi finalizada
            synchronized (MainAux.isPreparingToRetrieveFilesFromFtp) {
                MainAux.isPreparingToRetrieveFilesFromFtp.set(false);
                MainAux.isPreparingToRetrieveFilesFromFtp.notifyAll();
            }

            if (MainAux.arquivosParaEnviar.isEmpty()) {
                logger.warn("Nenhum arquivo disponibilizado no FTP para realizar download");
            }

            // Atualiza o status
            synchronized (MainAux.status) {
                MainAux.status.set("Baixando arquivos do FTP...");
                MainAux.status.notifyAll();
            }

            // Baixa os arquivos

            for (FileWrapper fileBase : MainAux.arquivosParaEnviar) {

                // Somente baixa arquivos que ainda estáo indisponíveis na máquina local
                if (fileBase.getStatusLocal() != FileStatusLocal.INDISPONIVEL) {
                    continue;
                }

                if (EnumSet.of(FileStatusRemoto.REFERENCIA_PERDIDA, FileStatusRemoto.NECESSARIO_VERIFICACAO, FileStatusRemoto.EM_TRANSFERENCIA).contains(fileBase.getStatusFtp())) {
                    detalheArquivo = MainAux.getDetalhesArquivo(fileBase);
                    arquivosPerdidos.append(fileBase.getFtpCaminhoCompleto()).append(System.lineSeparator());
                    arquivosPerdidosDetalhado.append(detalheArquivo).append(System.lineSeparator());
                    continue;
                }

                fileBase.setLocalCaminhoBase(Caminhos.DIRETORIO_ARQUIVOS_LOCAL + File.separator + fileBase.getParent() + File.separator + Dates.BRAZILIAN_DATE_FORMATTER_FS.format(LocalDate.ofInstant(MainAux.ultimoDownloadArquivosInstant, Dates.ZONE_ID)));

                tentativas = 0;
                tentativasDelete = 0;
                resultado = false;
                resultadoTemporario = false;
                gravarLog = true;
                inputStream = null;
                outputStream = null;
                relatedFiles = null;
                relatedFilesEnviados = null;
                error = "";

                // Se estiver configurado para validar o hash, busca o hash do arquivo no banco de dados
                if (Main.validaHashArquivoDb) {

                    logger.debug("Procurando hash para o arquivo " + fileBase.getNomeArquivo() + " no banco de dados");

                    try {
                        transitoDao.atualizaArquivoComDadosDoBd(fileBase);

                        if (!fileBase.possuiHashDatabase()) {
                            error = "Hash do arquivo não encontrado no banco de dados";
                            resultado = false;
                            logger.warn(error);
                        }
                    } catch (ArquivoStatusDbInvalidoException ex) {

                    } catch (Exception ex) {
                        error = "Erro ao buscar hash do arquivo no banco de dados";
                        resultado = false;
                        logger.error(error, ex);
                    }
                }

                // Tenta baixar o arquivo até o número máximo de tentativas ou se o resultado for positivo
                // E se somente se, estiver validando hash e o hash foi encontrado no banco de dados para o arquivo
                // Ou se não estiver validando hash
                while (tentativas++ <= Retries.MAX_RETRY_DOWNLOAD_FILE && !resultado && (!Main.validaHashArquivoDb || (Main.validaHashArquivoDb && fileBase.possuiHashDatabase()))) {

                    if (tentativas > 1) {
                        logger.debug("Iniciando tentativa " + tentativas + " de baixar o arquivo " + fileBase.getNomeArquivo() + " do FTP");
                    }

                    // Move para a pasta de trabalho e muda para a pasta do arquivo
                    if (!FTPClient.changeWorkingDirectory(fileBase.getFtpCaminhoBase())) {
                        logger.error(String.format("Tentativa %d. Erro ao mudar de pasta para baixar o arquivo %s do FTP", tentativas, fileBase.getFtpCaminhoCompleto()));
                        continue;
                    }

                    // Verifica se diretório de arquivos locais existe e cria se não existir
                    if (!FileUtils.createDirectoryIfNotExists(fileBase.getLocalCaminhoBase())) {
                        logger.error(String.format("Erro ao criar o diretório de arquivos locais %s", fileBase.getLocalCaminhoBase()));
                    }

                    // Renomeia o arquivo local se já existir
                    if (tentativas == 1) {
                        try {
                            if (FileUtils.tryOldifyFile(fileBase.getLocalCaminhoCompleto())) {
                                logger.info(String.format("Arquivo %s já existia e foi renomeado", fileBase.getLocalCaminhoCompleto()));
                            }
                        } catch (IOException e) {
                            logger.error(String.format("Erro ao tentar renomear o arquivo %s para %s.old", fileBase.getLocalCaminhoCompleto(), fileBase.getLocalCaminhoCompleto()), e);
                            break;
                        }
                    }

                    // Cria o arquivo local
                    try {
                        outputStream = new FileOutputStream(fileBase.getLocalCaminhoCompleto());
                    } catch (FileNotFoundException e) {
                        logger.error(String.format("Erro ao criar o arquivo %s local para download do FTP", fileBase.getLocalCaminhoCompleto()), e);
                        continue;
                    }

                    try {
                        try {

                            synchronized (MainAux.isDownloadingFileFtp) {
                                MainAux.isDownloadingFileFtp.set(true);
                                MainAux.isDownloadingFileFtp.notifyAll();
                            }

                            downloadWaitThread = ProcessaUtils.createThread("Arquivos Downloader Waiting", () -> {

                                int i = 0, parcial = 0;
                                int parciais = 5;

                                synchronized (MainAux.isDownloadingFileFtp) {
                                    while (MainAux.isDownloadingFileFtp.get()) {
                                        try {
                                            Thread.sleep(1000);
                                        } catch (InterruptedException e) {
                                            break;
                                        }

                                        if (i++ >= (Timings.TEMPO_LIMITE_DOWNLOAD.toSeconds() / parciais)) {
                                            logger.error(String.format("Aguarde, o programa está baixando o arquivo %s (%d/%d do tempo limite (%d ms))", fileBase.getFtpCaminhoCompleto(), ++parcial, parciais, Timings.TEMPO_LIMITE_DOWNLOAD.toMillis()));
                                            i = 0;
                                        }
                                    }
                                }
                            });


                            inputStream = FTPClient.getInstance().retrieveFileStream(fileBase.getNomeArquivo());

                            if (inputStream != null) {
                                // Transfere o arquivo para stream de saída
                                inputStream.transferTo(outputStream);
                            }

                            resultadoTemporario = FTPClient.getInstance().completePendingCommand();

                        } catch (IOException e) {
                            logger.error(String.format("Tentativa %d. Erro ao completar o comando pendente para baixar o arquivo %s do FTP", tentativas, fileBase.getFtpCaminhoCompleto()), e);
                            resultadoTemporario = false;
                        } finally {

                            try {
                                if (downloadWaitThread != null) {
                                    downloadWaitThread.interrupt();
                                }
                            } catch (Exception e) {
                                logger.error("Erro ao interromper a thread de espera do download do arquivo do FTP", e);
                            }

                            // Fecha os streams
                            try {
                                if (inputStream != null) {
                                    inputStream.close();
                                }
                            } catch (Exception ex) {
                                logger.error(String.format("Tentativa %d. Erro ao fechar o stream de entrada após erro de download do FTP. ", tentativas), ex);
                            }

                            try {
                                outputStream.flush();
                                outputStream.close();
                            } catch (Exception ex) {
                                logger.error(String.format("Tentativa %d. Erro ao fechar o stream de saída após erro de download do FTP. ", tentativas), ex);
                            }

                            synchronized (MainAux.isDownloadingFileFtp) {
                                MainAux.isDownloadingFileFtp.set(false);
                                MainAux.isDownloadingFileFtp.notifyAll();
                            }
                        }

                        // Baixa o arquivo e verifica se foi baixado corretamente
                        if (resultadoTemporario && inputStream != null) {

                            // Verifica se o tamanho do arquivo baixado é igual ao tamanho do arquivo no FTP
                            if (Files.size(Path.of(fileBase.getLocalCaminhoCompleto())) == fileBase.getFtpFile().getSize()) {
                                resultado = true;
                            } else {
                                error = "Tamanho do arquivo baixado diferente do tamanho do arquivo no FTP";
                                resultado = false;
                            }

                            // Verifica se o hash do arquivo baixado é igual ao hash do arquivo no banco de dados
                            if (Main.validaHashArquivoDb && resultado) {
                                try {
                                    if (!fileBase.comparaHashDatabase()) {
                                        error = "Hash do arquivo baixado diferente do hash do arquivo no banco de dados";
                                        resultado = false;
                                    }
                                } catch (Exception ex) {
                                    error = "Erro ao comparar o hash do arquivo baixado com o hash do arquivo no banco de dados.";
                                    resultado = false;
                                }
                            }

                            // Se o arquivo não foi baixado corretamente, exclui o arquivo local
                            if (!resultado) {
                                logger.error(String.format("Tentativa %d. Erro ao baixar o arquivo \"%s\" do FTP. %s", tentativas, fileBase.getNomeArquivo(), error));

                                tentativasDelete = 0;

                                while (tentativasDelete++ <= Retries.MAX_RETRY_DELETE_FILE) {
                                    try {
                                        Files.deleteIfExists(Path.of(fileBase.getLocalCaminhoCompleto()));
                                        break;
                                    } catch (IOException ex) {
                                        logger.error(String.format("Tentativa %d - %d. Erro ao excluir o arquivo \"%s\" local após erro de download do FTP. ", tentativas, tentativasDelete, fileBase.getNomeArquivo()), ex);
                                    }
                                }
                            } else { // Realiza uma última validação procurando arquivos relacionados ao arquivo baixado para verificar se o arquivo já foi enviado para o Connect

                                try {
                                    // Procura arquivos relacionados ao arquivo baixado na mesma pasta
                                    relatedFiles = Objects.requireNonNullElse(FileUtils.findRelatedFiles(fileBase.getLocalCaminhoBase(), fileBase.getNomeArquivo()), new ArrayList<>());

                                    // Procura arquivos enviados
                                    relatedFilesEnviados = relatedFiles.stream().filter(f -> f.getFileName().toString().endsWith(Caminhos.SUFIXO_ARQUIVO_ENVIADO)).toList();

                                    // Se existirem arquivos relacionados
                                    if (!relatedFilesEnviados.isEmpty()) {


                                        if (Files.size(Path.of(fileBase.getLocalCaminhoCompleto())) != Files.size(relatedFilesEnviados.get(0))) {  // Se o arquivo que foi enviado for DIFERENTE EM TAMANHO

                                            logger.warn(String.format("Tentativa %d. Arquivo %s baixado do FTP já foi enviado para o Connect. Porém o tamanho dos dois arquivos não conferem.", tentativas, fileBase.getFtpCaminhoCompleto()));
                                            logger.trace(String.format("Tentativa %d. Tamanho do arquivo baixado \"%s\" diferente do tamanho do arquivo enviado \"%s\"", tentativas, fileBase.getNomeArquivo(), relatedFilesEnviados.get(0).getFileName().toString()));
                                            logger.trace(String.format("Tentativa %d. O programa seguirá fluxo normal, pois o arquivo enviado tem tamanho diferente do arquivo baixado. O arquivo será enviado novamente", tentativas));

                                        } else if (!FileUtils.calculateFileHash1(fileBase.getLocalCaminhoCompleto()).equalsIgnoreCase(FileUtils.calculateFileHash1(relatedFilesEnviados.get(0).toString()))) { // Se o arquivo que foi enviado for DIFERENTE EM HASH

                                            logger.warn(String.format("Tentativa %d. Arquivo %s baixado do FTP já foi enviado para o Connect. Porém o hash dos dois arquivos não conferem.", tentativas, fileBase.getFtpCaminhoCompleto()));
                                            logger.trace(String.format("Tentativa %d. Hash do arquivo baixado \"%s\" diferente do hash do arquivo enviado \"%s\"", tentativas, fileBase.getNomeArquivo(), relatedFilesEnviados.get(0).getFileName().toString()));
                                            logger.trace(String.format("Tentativa %d. O programa seguirá fluxo normal, pois o arquivo enviado tem hash diferente do arquivo baixado. O arquivo será enviado novamente", tentativas));

                                        } else if (!fileBase.getNomeArquivo().equalsIgnoreCase(relatedFilesEnviados.get(0).getFileName().toString().replace(Caminhos.SUFIXO_ARQUIVO_ENVIADO, ""))) {

                                            logger.warn(String.format("Tentativa %d. Arquivo %s baixado do FTP já foi enviado para o Connect. Porém o nome dos dois arquivos não conferem.", tentativas, fileBase.getFtpCaminhoCompleto()));
                                            logger.trace(String.format("Tentativa %d. Nome do arquivo baixado \"%s\" diferente do nome do arquivo enviado \"%s\"", tentativas, fileBase.getNomeArquivo(), relatedFilesEnviados.get(0).getFileName().toString()));
                                            logger.trace(String.format("Tentativa %d. O programa seguirá fluxo normal, pois o arquivo enviado tem nome diferente do arquivo baixado. O arquivo será enviado novamente", tentativas));

                                        } else { // Se o arquivo que foi enviado for IGUAL EM NOMENCLATURA, HASH e TAMANHO

                                            logger.warn(String.format("Tentativa %d. Arquivo %s baixado do FTP já foi enviado para o Connect. O arquivo baixado será excluído e será mantido o existente na pasta.", tentativas, fileBase.getFtpCaminhoCompleto()));

                                            // Se o arquivo que foi enviado for IGUAL em TAMANHO, HASH e NOMENCLATURA ao arquivo baixado, marca o arquivo como JÁ ENVIADO
                                            fileBase.setStatusLocalSilent(FileStatusLocal.DISPONIVEL);
                                            fileBase.setStatusEnvioSilent(FileStatusEnvio.ENVIADO);

                                            tentativasDelete = 0;

                                            while (tentativasDelete++ <= Retries.MAX_RETRY_DELETE_FILE) {
                                                try {
                                                    logger.trace(String.format("Tentativa %d - %d. Excluindo o arquivo baixado \"%s\" do FTP, pois o arquivo já foi enviado para o Connect e está localizado em %s", tentativas, tentativasDelete, fileBase.getLocalCaminhoCompleto(), fileBase.getLocalCaminhoCompleto() + "." + Caminhos.SUFIXO_ARQUIVO_ENVIADO));
                                                    Files.deleteIfExists(Path.of(fileBase.getLocalCaminhoCompleto()));
                                                    break;
                                                } catch (IOException ex) {
                                                    logger.error(String.format("Tentativa %d - %d. Erro ao excluir o arquivo \"%s\" local após erro de download do FTP. ", tentativas, tentativasDelete, fileBase.getNomeArquivo()), ex);
                                                }
                                            }

                                            gravarLog = false;
                                        }
                                    }
                                } catch (Exception ex) {
                                    logger.trace(String.format("Tentativa %d. Erro ao verificar arquivos relacionados ao arquivo baixado \"%s\"", tentativas, fileBase.getNomeArquivo()), ex);
                                }
                            }

                        } else {


                            logger.error(String.format("Tentativa %d. Erro ao baixar o arquivo %s do FTP. ", tentativas, fileBase.getFtpCaminhoCompleto()));

                            // Se o arquivo não foi baixado corretamente, exclui o arquivo local

                            tentativasDelete = 0;

                            while (tentativasDelete++ <= Retries.MAX_RETRY_DELETE_FILE) {
                                try {
                                    Files.deleteIfExists(Path.of(fileBase.getLocalCaminhoCompleto()));
                                    break;
                                } catch (IOException ex) {
                                    logger.error(String.format("Tentativa %d - %d. Erro ao excluir o arquivo \"%s\" local após erro de download do FTP. ", tentativas, tentativasDelete, fileBase.getFtpCaminhoCompleto()), ex);
                                }
                            }
                        }
                    } catch (Exception e) {
                        logger.error(String.format("Tentativa %d. Erro ao baixar o arquivo %s do FTP", tentativas, fileBase.getFtpCaminhoCompleto()), e);

                        try {
                            if (inputStream != null)
                                inputStream.close();
                        } catch (Exception ex) {
                            logger.error(String.format("Tentativa %d. Erro ao fechar o stream de entrada após erro de download do FTP. ", tentativas), ex);
                        }

                        try {
                            outputStream.flush();
                            outputStream.close();
                        } catch (Exception ex) {
                            logger.error(String.format("Tentativa %d. Erro ao fechar o stream de saída após erro de download do FTP. ", tentativas), ex);
                        }

                        tentativasDelete = 0;

                        while (tentativasDelete++ <= Retries.MAX_RETRY_DELETE_FILE) {
                            try {
                                Files.deleteIfExists(Path.of(fileBase.getLocalCaminhoCompleto()));
                                break;
                            } catch (IOException ex) {
                                logger.error(String.format("Tentativa %d - %d. Erro ao excluir o arquivo %s local após erro de download do FTP. ", tentativas, tentativasDelete, fileBase.getLocalCaminhoCompleto()), ex);
                            }
                        }

                        error = e.getMessage();
                    }
                }

                if (!gravarLog) continue;

                if (resultado) {
                    fileBase.setStatusLocal(FileStatusLocal.DISPONIVEL);
                    fileBase.setAdicional(null);
                    detalheArquivo = MainAux.getDetalhesArquivo(fileBase);
                    arquivosBaixados.append(fileBase.getFtpCaminhoCompleto()).append(System.lineSeparator());
                    arquivosBaixadosDetalhado.append(detalheArquivo).append(System.lineSeparator());
                } else {
                    fileBase.setStatusLocal(FileStatusLocal.INDISPONIVEL);
                    fileBase.setAdicional(error);
                    detalheArquivo = MainAux.getDetalhesArquivo(fileBase);
                    arquivosQueDeramErro.append(fileBase.getFtpCaminhoCompleto()).append(System.lineSeparator());
                    arquivosQueDeramErroDetalhado.append(detalheArquivo);
                    arquivosQueDeramErroDetalhado.append(System.lineSeparator());
                }
            }

            if (Timings.DELAY_AFTER_DOWNLOAD_FILES != null)
                Thread.sleep(Timings.DELAY_AFTER_DOWNLOAD_FILES);

            excluirArquivoLock(lockFileName);

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

                if (!gravouArquivosBaixadosDia) {
                    try {
                        if (!arquivosBaixados.isEmpty()) {
                            Files.writeString(caminhoLogArquivosBaixadosDia, arquivosBaixadosDetalhado.toString(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                        }

                        gravouArquivosBaixadosDia = true;
                    } catch (IOException e) {
                        logger.error("Erro ao gravar arquivos enviados do dia em " + caminhoLogArquivosBaixadosDia, e);
                        gravouArquivosEnviadosDiaException = e;
                    }
                }

                if (!gravouArquivosPerdidosDia) {
                    try {
                        if (!arquivosPerdidos.isEmpty()) {
                            Files.writeString(caminhoLogArquivosPerdidosDia, arquivosPerdidosDetalhado.toString(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                        }

                        gravouArquivosPerdidosDia = true;
                    } catch (IOException e) {
                        logger.error("Erro ao gravar arquivos perdidos do dia em " + caminhoLogArquivosPerdidosDia, e);
                        gravouArquivosPerdidosDiaException = e;
                    }
                }

                if (!gravouArquivosBaixadosExecucao) {
                    try {
                        if (!arquivosBaixados.isEmpty()) {
                            Files.writeString(caminhoLogArquivosBaixadosExecucao, arquivosBaixadosDetalhado.toString(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                        }

                        gravouArquivosBaixadosExecucao = true;
                    } catch (IOException e) {
                        logger.error("Erro ao gravar arquivos enviados da execucao em " + caminhoLogArquivosBaixadosExecucao, e);
                        gravouArquivosEnviadosExecucaoException = e;
                    }
                }

                if (!gravouArquivosPerdidosExecucao) {
                    try {

                        if (!arquivosPerdidos.isEmpty()) {
                            Files.writeString(caminhoLogArquivosPerdidosExecucao, arquivosPerdidosDetalhado.toString(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                        }

                        gravouArquivosPerdidosExecucao = true;
                    } catch (IOException e) {
                        logger.error("Erro ao gravar arquivos perdidos da execucao em " + caminhoLogArquivosPerdidosExecucao, e);
                        gravouArquivosPerdidosExecucaoException = e;
                    }
                }

                if (!gravouArquivosQueDeramErroExecucao) {
                    try {
                        if (!arquivosQueDeramErro.isEmpty()) {
                            Files.writeString(caminhoLogArquivosComErroExecucao, arquivosQueDeramErroDetalhado.toString(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                        }

                        gravouArquivosQueDeramErroExecucao = true;
                    } catch (IOException e) {
                        logger.error("Erro ao gravar arquivos que deram erro da execução em " + caminhoLogArquivosComErroExecucao, e);
                        gravouArquivosQueDeramErroExecucaoException = e;
                    }
                }

                if (!gravouArquivosQueDeramErroDia) {
                    try {
                        if (!arquivosQueDeramErro.isEmpty()) {
                            Files.writeString(caminhoLogArquivosComErroDia, arquivosQueDeramErroDetalhado.toString(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                        }

                        gravouArquivosQueDeramErroDia = true;
                    } catch (IOException e) {
                        logger.error("Erro ao gravar arquivos que deram erro do dia em " + caminhoLogArquivosComErroDia, e);
                        gravouArquivosQueDeramErroDiaException = e;
                    }
                }

                // Se todos os arquivos foram gravados, sai do loop
                if (gravouArquivosBaixadosDia && gravouArquivosPerdidosDia && gravouArquivosBaixadosExecucao && gravouArquivosPerdidosExecucao && gravouArquivosQueDeramErroExecucao && gravouArquivosQueDeramErroDia) {
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
                if (!gravouArquivosBaixadosDia || !gravouArquivosPerdidosDia || !gravouArquivosBaixadosExecucao || !gravouArquivosPerdidosExecucao || !gravouArquivosQueDeramErroExecucao || !gravouArquivosQueDeramErroDia) {
                    logger.error("Erro ao gravar arquivos baixados e perdidos");

                    StringBuilder body = new StringBuilder();

                    if (!gravouArquivosBaixadosDia) {
                        body.append("❌ Erro ao atualizar arquivos baixados do dia em ").append(caminhoLogArquivosBaixadosDia).append(System.lineSeparator());
                        body.append("Exception: ").append(gravouArquivosEnviadosDiaException).append(System.lineSeparator()).append(System.lineSeparator());
                    }

                    if (!gravouArquivosPerdidosDia) {
                        body.append("❌ Erro ao atualizar arquivos perdidos do dia em ").append(caminhoLogArquivosPerdidosDia).append(System.lineSeparator());
                        body.append("Exception: ").append(gravouArquivosPerdidosDiaException).append(System.lineSeparator()).append(System.lineSeparator());
                    }

                    if (!gravouArquivosQueDeramErroDia) {
                        body.append("❌ Erro ao gravar arquivos que deram erro do dia em ").append(caminhoLogArquivosComErroDia).append(System.lineSeparator());
                        body.append("Exception: ").append(gravouArquivosQueDeramErroDiaException).append(System.lineSeparator()).append(System.lineSeparator());
                    }

                    if (!gravouArquivosBaixadosExecucao) {
                        body.append("❌ Erro ao gravar arquivos baixados da execução " + MainAux.idFilesRetrieval.get() + " em ").append(caminhoLogArquivosBaixadosExecucao).append(System.lineSeparator());
                        body.append("Exception: ").append(gravouArquivosEnviadosExecucaoException).append(System.lineSeparator()).append(System.lineSeparator());
                    }

                    if (!gravouArquivosPerdidosExecucao) {
                        body.append("❌ Erro ao gravar arquivos perdidos da execução" + MainAux.idFilesRetrieval.get() + " em ").append(caminhoLogArquivosPerdidosExecucao).append(System.lineSeparator());
                        body.append("Exception: ").append(gravouArquivosPerdidosExecucaoException).append(System.lineSeparator()).append(System.lineSeparator());
                    }

                    if (!gravouArquivosQueDeramErroExecucao) {
                        body.append("❌ Erro ao gravar arquivos que deram erro da execução " + MainAux.idFilesRetrieval.get() + " em ").append(caminhoLogArquivosComErroExecucao).append(System.lineSeparator());
                        body.append("Exception: ").append(gravouArquivosQueDeramErroExecucaoException).append(System.lineSeparator()).append(System.lineSeparator());
                    }

                    Mailer.getInstance().sendMail(TopicEmail.ALERTA_GRAVACAO_LOG_ARQUIVOS, body.toString());
                }
            } catch (Exception e) {
                logger.error("Erro ao enviar email de alerta de erro ao gravar arquivos baixados e perdidos", e);
            }

            try {
                if (!arquivosQueDeramErro.isEmpty()) {

                    StringBuilder body = new StringBuilder();

                    body.append(DateUtils.getCumprimento()).append(",").append(System.lineSeparator()).append(System.lineSeparator());
                    body.append("❌ Os seguintes arquivos apresentaram erro ao serem baixados do FTP às ").append(Dates.BRAZILIAN_DATE_TIME_FORMATTER.format(LocalDateTime.ofInstant(MainAux.ultimoDownloadArquivosInstant, Dates.ZONE_ID))).append(":").append(System.lineSeparator()).append(System.lineSeparator());
                    body.append(arquivosQueDeramErro).append(System.lineSeparator());
                    body.append("▶️ Para mais detalhes, verifique o(s) motivo(s) do(s) erro(s) em ").append(caminhoLogArquivosComErroExecucao);

                    Mailer.getInstance().sendMail(TopicEmail.ALERTA_DOWNLOAD_ARQUIVOS_FTP, body.toString());
                }
            } catch (Exception e) {
                logger.error("Erro ao enviar email de alerta de erro ao baixar arquivos do FTP", e);
            }

            // Atualiza o status
            synchronized (MainAux.status) {
                MainAux.status.set("Arquivos baixados do FTP e disponiblizados para envio...");
                MainAux.status.notifyAll();
            }
        } catch (Exception e) {
            logger.error("Erro ao baixar arquivos do FTP", e);
        } finally {

            // Notifica que a preparacao para enviar para o Connect foi finalizada
            synchronized (MainAux.isPreparingToRetrieveFilesFromFtp) {
                MainAux.isPreparingToRetrieveFilesFromFtp.set(false);
                MainAux.isPreparingToRetrieveFilesFromFtp.notifyAll();
            }

            // Notifica que o envio para o Connect foi finalizado
            synchronized (MainAux.isDownloadingFilesFtp) {
                MainAux.isDownloadingFilesFtp.set(false);
                MainAux.isDownloadingFilesFtp.notifyAll();
            }
        }
    }

    /**
     * Funcao que cria o nome do arquivo de lock
     *
     * @return nome do arquivo de lock
     */
    private static String createLockFileName() {
        return "LOCK_" + Dates.BRAZILIAN_DATE_TIME_FORMATTER_FS.format(LocalDateTime.ofInstant(Instant.now(), Dates.ZONE_ID)) + ".LOCK";
    }


    /**
     * Funcao que grava arquivo de Lock no FTP para sinalizar pro programa do setor de Geração nao conseguir enviar nada durante o lock
     */
    private static void gravarArquivoLock(String lockFileName) {
        try {

            FTPClient.moveToWorkBaseDirectory();

            FTPClient.getInstance().storeFile(lockFileName, new ByteArrayInputStream(new byte[0]));

            logger.info("Arquivo de lock do FTP gravado com sucesso em " + FTPClient.getInstance().printWorkingDirectory() + "/" + lockFileName);
        } catch (Exception e) {
            logger.error("Erro ao gravar arquivo de lock do FTP", e);
        }
    }

    /**
     * Funcao que exclui arquivo de Lock no FTP para sinalizar pro programa do setor de Geração que ja pode enviar arquivos
     */
    private static void excluirArquivoLock(String lockFileName) {

        int tentativas = 0;

        while (tentativas++ <= Retries.MAX_RETRY_DELETE_FILE) {
            try {
                FTPClient.moveToWorkBaseDirectory();

                FTPClient.getInstance().deleteFile(lockFileName);

                logger.info("Arquivo de lock deletado do FTP com sucesso em " + FTPClient.getInstance().printWorkingDirectory() + "/" + lockFileName);
                break;
            } catch (Exception e) {
                logger.error(String.format("Tentativa %d Erro ao excluir arquivo de lock do FTP", tentativas), e);
            }
        }
    }

    /**
     * Funcao que verifica se é obrigatorio procurar por novos arquivos e é um impedimento para enviar os arquivos
     * É obrigatorio procurar por arquivos se algum arquivo estiver em transferencia
     * A fim de evitar que um arquivo seja enviado pela metade, é necessario verificar se o arquivo está em transferencia
     *
     * @return true se é obrigatorio procurar por novos arquivos
     */
    private boolean isObrigatorioPesquisarNovamente() {
        return (MainAux.arquivosParaEnviar.stream().anyMatch(a -> a.getStatusFtp() == FileStatusRemoto.EM_TRANSFERENCIA) || MainAux.arquivosParaEnviar.stream().anyMatch(a -> a.getStatusFtp() == FileStatusRemoto.NECESSARIO_VERIFICACAO));
    }

    /**
     * Funcao que verifica se precisa procurar por novos arquivos pelo menos uma vez
     * @return
     */
    private boolean devePesquisarPeloMenosUmaVez () {

        return (MainAux.ultimaBuscaArquivosInstant == null ||
                Duration.between(MainAux.ultimaBuscaArquivosInstant, Instant.now()).toMinutes() > 1 ||
                MainAux.arquivosParaEnviar.isEmpty() ||
                isObrigatorioPesquisarNovamente() ||
                MainAux.arquivosParaEnviar.stream().anyMatch(a -> a.getStatusFtp() == FileStatusRemoto.REFERENCIA_PERDIDA && a.getStatusEnvio() != FileStatusEnvio.ENVIADO && a.getStatusLocal() != FileStatusLocal.DISPONIVEL)
        );
    }
}
