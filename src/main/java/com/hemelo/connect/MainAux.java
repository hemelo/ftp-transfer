package com.hemelo.connect;

import com.hemelo.connect.constants.Caminhos;
import com.hemelo.connect.constants.Dates;
import com.hemelo.connect.constants.Retries;
import com.hemelo.connect.constants.Timings;
import com.hemelo.connect.dto.FileWrapper;
import com.hemelo.connect.enums.FileStatusEnvio;
import com.hemelo.connect.enums.FileStatusLocal;
import com.hemelo.connect.enums.FileStatusRemoto;
import com.hemelo.connect.exception.ConexaoException;
import com.hemelo.connect.infra.Datasource;
import com.hemelo.connect.infra.FTPClient;
import com.hemelo.connect.utils.DateUtils;
import com.hemelo.connect.utils.FileUtils;
import com.hemelo.connect.utils.ProcessaUtils;
import com.hemelo.connect.utils.PropertiesUtils;
import dorkbox.systemTray.MenuItem;
import dorkbox.systemTray.SystemTray;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import javax.swing.*;
import java.awt.*;
import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.*;
import java.util.*;
import java.util.Timer;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Classe auxiliar para o programa
 */
public abstract class MainAux {

    public static final Logger logger = org.slf4j.LoggerFactory.getLogger(MainAux.class);
    public static Thread serverThread, statusThread, dsThread, arquivosDownloaderThread;
    public static Timer arquivosFinderTimer, arquivosSenderTimer, minuteTimer, dailyTimer, hourlyTimer;

    public static Instant ultimaBuscaArquivosInstant, ultimoDownloadArquivosInstant, ultimoEnvioArquivosInstant;
    
    public static ConcurrentLinkedQueue<FileWrapper> arquivosParaEnviar = new ConcurrentLinkedQueue<>();

    public static final AtomicReference<String> status = new AtomicReference<>("");
    public static final AtomicInteger idFilesRetrieval = new AtomicInteger(0);
    public static final AtomicInteger idFilesSend = new AtomicInteger(0);
    public static final AtomicBoolean isCleaningLogs = new AtomicBoolean(false);
    public static final AtomicBoolean isResettingData = new AtomicBoolean(false);
    public static final AtomicBoolean isSendingRelatorios = new AtomicBoolean(false);
    public static final AtomicBoolean isSendingFilesToConnect = new AtomicBoolean(false);
    public static final AtomicBoolean isProcurandoArquivos = new AtomicBoolean(false);
    public static final AtomicBoolean isDownloadingFilesFtp = new AtomicBoolean(false);
    public static final AtomicBoolean isDownloadingFileFtp = new AtomicBoolean(false);
    public static final AtomicBoolean isPreparingToRetrieveFilesFromFtp = new AtomicBoolean(false);

    public static final ReentrantLock lockProcessamento = new ReentrantLock();

    private static String usuario = System.getProperty("user.name");
    private static Datasource datasource;

    private MainAux() {
        throw new IllegalStateException("Utility class");
    }

    //------------------------------------------------------------------------------------------------------------------

    /**
     * Impressão das configurações do programa
     */
    public static void imprimeConfiguracoes() {
        logger.debug("Ambiente: " + getEnvironment());
        logger.debug("Validar hash dos arquivos baixados: " + Main.validaHashArquivoDb);
        logger.debug("Enviar emails: " + Main.enviarEmails);
        logger.debug("Intervalo de pesquisa de arquivos: {}ms", Timings.INTERVALO_PESQUISA_ARQUIVOS.toMillis());
        logger.debug("Intervalo de pesquisa de arquivos antes de uma nova tentativa durante o download de arquivos: {}ms", Timings.INTERVALO_PESQUISA_ARQUIVOS_BEFORE_RETRIEVAL_MS);
        logger.debug("Intervalo de pesquisa de arquivos locais para envio ao Connect: {}ms", Timings.INTERVALO_PESQUISA_ARQUIVOS_LOCAIS_ENVIO_CONNECT_MS);
        logger.debug("Intervalo de erro IO retry: {}ms", Timings.INTERVALO_ERRO_IO_RETRY);
        logger.debug("Horários de download de arquivos do FTP: {}", Objects.requireNonNullElse(Timings.HORARIOS_DOWNLOAD_ARQUIVOS_FTP, new ArrayList<LocalTime>()).stream().map(Dates.HOUR_FORMATTER::format).toList());
        logger.debug("Delay após download dos arquivos do FTP: {}ms", Timings.DELAY_AFTER_DOWNLOAD_FILES);
        logger.debug("Tempo limite para download de arquivos: {}ms", Timings.TEMPO_LIMITE_DOWNLOAD.toMillis());
        logger.debug("Usuário logado: {}", getUsuario());
        logger.debug("Caminho local de arquivos: \"{}\"", Caminhos.DIRETORIO_ARQUIVOS_LOCAL);
        logger.debug("Caminho de busca no FTP: \"{}\"", Caminhos.CAMINHO_BASE_FTP);
        logger.debug("Caminho de envio do Connect: \"{}\"", Caminhos.CAMINHO_ENVIO_CONNECT);
        logger.debug("Caminho dos arquivos de log4j: \"{}\"",getCaminhoLogAplicacao());
        logger.debug("Caminho dos arquivos de log: \"{}\"", getCaminhoArquivosLog());
        logger.debug("Caminho dos arquivos de log diários: \"{}\"", getCaminhoArquivosLogDiarios());
    }

    /**
     * Valida os arquivos de propriedades
     */
    static void validaProperties () {

        synchronized (status) {
            status.set("Validando arquivos de propriedades...");
            status.notifyAll();
        }

        Optional<Properties> log4j = PropertiesUtils.getProperties("log4j2.properties");
        Optional<Properties> amqp = PropertiesUtils.getProperties("amqp.properties");
        Optional<Properties> ldap = PropertiesUtils.getProperties("ldap.properties");
        Optional<Properties> db = PropertiesUtils.getProperties("database.properties");
        Optional<Properties> mail = PropertiesUtils.getProperties("mail.properties");

        if (log4j.isEmpty()) {
            System.err.println("Arquivo de propriedades log4j2.properties não encontrado");
        }

        if (amqp.isEmpty()) {
            if (log4j.isEmpty()) {
                System.err.println("Arquivo de propriedades amqp.properties não encontrado");
            } else {
                logger.error("Arquivo de propriedades amqp.properties não encontrado");
            }
        }

        if (ldap.isEmpty()) {
            if (log4j.isEmpty()) {
                System.err.println("Arquivo de propriedades ldap.properties não encontrado");
            } else {
                logger.error("Arquivo de propriedades ldap.properties não encontrado");
            }
        }

        if (db.isEmpty()) {
            if (log4j.isEmpty()) {
                System.err.println("Arquivo de propriedades database.properties não encontrado");
            } else {
                logger.error("Arquivo de propriedades database.properties não encontrado");
            }
        }

        if (mail.isEmpty()) {
            if (log4j.isEmpty()) {
                System.err.println("Arquivo de propriedades mail.properties não encontrado");
            } else {
                logger.error("Arquivo de propriedades mail.properties não encontrado");
            }
        }

        synchronized (status) {
            status.set("");
            status.notifyAll();
        }
    }

    /**
     * Cria o SystemTray com as opções disponíveis
     */
    static void createSystemTray() {

        AtomicReference<Optional<SystemTray>> st = new AtomicReference<>(null);

        SwingUtilities.invokeLater(() -> {

            SystemTray.DEBUG = true;

            st.set(Optional.ofNullable(SystemTray.get()));

            if (st.get().isEmpty()) {
                logger.error("SystemTray not supported");
                return;
            }
            st.get().get().setEnabled(true);

            st.get().get().setTooltip("Connect - FTP Transfer");

            try {
                st.get().get().setImage(Objects.requireNonNull(Main.class.getResource("/images/logo.jpg")));
            } catch (Exception e) {
                logger.error("Erro ao carregar a imagem do SystemTray", e);
            }

            String logFile = getCaminhoLogAplicacao();

            if (StringUtils.isNotBlank(logFile)) {

                File file = new File(logFile);

                if (file.exists()) {
                    st.get().get().getMenu().add(new MenuItem("Visualizar Logs", x -> {
                        try {
                            Desktop desktop = Desktop.getDesktop();
                            desktop.open(file);
                        } catch (Exception e) {
                            logger.error("Erro ao abrir o arquivo de logs", e);
                        }
                    }));
                } else {
                    logger.error(String.format("Arquivo de logs não encontrado: %s", logFile));
                }
            }

            final AtomicReference<String> statusAnterior = new AtomicReference<>("");

            statusThread = ProcessaUtils.createLoopedThread("Status Thread", () -> {

                synchronized (status) {
                    st.get().ifPresent(tray -> tray.setStatus(status.get()));

                    if (StringUtils.isNotBlank(status.get())) {
                        logger.trace(String.format("Status ST atualizado: %s", status.get()));
                    }

                    while (statusAnterior.get().equals(status.get())) {
                        try {
                            status.wait();
                        } catch (InterruptedException e) {
                            logger.error("Erro ao aguardar a finalizacao da thread do status", e);
                        }
                    }

                    statusAnterior.set(status.get());
                }
            });
        });

        /*synchronized (st) {
            while (st.get() == null) {
                try {
                    st.wait();
                } catch (InterruptedException e) {
                    logger.error("Erro ao aguardar a finalizacao da thread do SystemTray", e);
                }
            }
        }*/
    }

    //------------------------------------------------------------------------------------------------------------------

    /**
     * Metodo responsavel para inicializar o banco ded dados
     *
     * @return
     */
    static void connectDatasource() {

        // Cria uma thread para conectar ao banco de dados
        // A conexao é feita em uma thread separada para evitar que a tela de aguarde fique travada
        dsThread = ProcessaUtils.createThread("Datasource Initializer", () -> {
            // Inicia a conexão com o banco de dados
            try {
                datasource = new Datasource(Main.isProductionEnvironment);

            } catch (
                    ConexaoException ex) { // Se ocorrer um erro ao conectar ao banco de dados, exibe uma mensagem de erro e finaliza o programa
                logger.error("Erro ao conectar ao banco de dados", ex.getCause() == null ? ex : ex.getCause());
            }

            Thread.currentThread().interrupt();
        });
    }

    static void waitForDatasource() {

        synchronized (status) {
            status.set("Conectando ao sistema...");
            status.notifyAll();
        }

        synchronized (Datasource.isConnectingDatasource) {
            while (Datasource.isConnectingDatasource.get()) { // Enquanto a conexao com o banco de dados estiver acontecendo, aguarda
                try {
                    Datasource.isConnectingDatasource.wait();
                } catch (InterruptedException e) {
                    logger.error("Erro ao aguardar a finalizacao da thread do datasource", e);
                }
            }
        }

        synchronized (status) {
            status.set("Conectado ao sistema");
            status.notifyAll();
        }
    }

    //------------------------------------------------------------------------------------------------------------------

    public static String getCaminhoLogAplicacao() {
        Optional<Properties> props = PropertiesUtils.getProperties("log4j2.properties");

        if (props.isPresent())
            return props.get().getProperty("property.basePath");

        return null;
    }

    public static String getCaminhoLogAplicacaoCompleto() {
        Optional<Properties> props = PropertiesUtils.getProperties("log4j2.properties");

        if (props.isPresent()) {
            String fileName = props.get().getProperty("property.fileName");
            fileName = fileName.substring(fileName.lastIndexOf(File.separator) + 1);
            return props.get().getProperty("property.basePath") + File.separator + fileName;
        }

        return null;
    }

    /**
     * Retorna o caminho base dos arquivos de log
     * @return
     */
    public static String getCaminhoArquivosLog() {
        return Caminhos.DIRETORIO_LOGS_FTP_DOWNLOAD;
    }

    /**
     * Retorna o caminho base dos arquivos de log diários
     * @return
     */
    public static String getCaminhoArquivosLogDiarios() {
        String caminhoLogs = getCaminhoArquivosLog() + File.separator + Dates.BRAZILIAN_DATE_FORMATTER_FS.format(LocalDate.ofInstant(Instant.now(), Dates.ZONE_ID));

        // Verifica se diretório de logs existe e cria se não existir
        FileUtils.createDirectoryIfNotExists(caminhoLogs);

        return caminhoLogs;
    }

    public static Path getCaminhoArquivosBaixadosExecucao() {
        assert ultimoDownloadArquivosInstant != null;
        Path caminhoLog = Path.of(getCaminhoArquivosLogDiarios() + File.separator + "ARQUIVOS_BAIXADOS_" + idFilesRetrieval.get() + "_" + Dates.BRAZILIAN_DATE_TIME_FORMATTER_FS.format(LocalDateTime.ofInstant(ultimoDownloadArquivosInstant, Dates.ZONE_ID)) + ".CSV");
        adicionaHeaderLogFile(caminhoLog);
        return caminhoLog;
    }

    public static Path getCaminhoArquivosBaixadosDia() {
        assert ultimoDownloadArquivosInstant != null;
        Path caminhoLog = Path.of(getCaminhoArquivosLogDiarios() + File.separator + "ARQUIVOS_BAIXADOS_" + Dates.BRAZILIAN_DATE_FORMATTER_FS.format(LocalDate.ofInstant(ultimoDownloadArquivosInstant, Dates.ZONE_ID)) + ".CSV");
        adicionaHeaderLogFile(caminhoLog);
        return caminhoLog;
    }

    public static Path getCaminhoArquivosEnviadosExecucao() {
        assert ultimoEnvioArquivosInstant != null;
        Path caminhoLog = Path.of(getCaminhoArquivosLogDiarios() + File.separator + "ARQUIVOS_ENVIADOS_" + idFilesSend.get() + "_" + Dates.BRAZILIAN_DATE_TIME_FORMATTER_FS.format(LocalDateTime.ofInstant(ultimoEnvioArquivosInstant, Dates.ZONE_ID)) + ".CSV");
        adicionaHeaderLogFile(caminhoLog);
        return caminhoLog;
    }

    public static Path getCaminhoArquivosEnviadosDia() {
        assert ultimoEnvioArquivosInstant != null;
        Path caminhoLog = Path.of(getCaminhoArquivosLogDiarios() + File.separator + "ARQUIVOS_ENVIADOS_" + Dates.BRAZILIAN_DATE_FORMATTER_FS.format(LocalDate.ofInstant(ultimoEnvioArquivosInstant, Dates.ZONE_ID)) + ".CSV");
        adicionaHeaderLogFile(caminhoLog);
        return caminhoLog;
    }

    public static Path getCaminhoArquivosPerdidosExecucao() {
        assert ultimoDownloadArquivosInstant != null;
        Path caminhoLog = Path.of(getCaminhoArquivosLogDiarios() + File.separator + "ARQUIVOS_PERDIDOS_" + idFilesRetrieval.get() + "_" + Dates.BRAZILIAN_DATE_TIME_FORMATTER_FS.format(LocalDateTime.ofInstant(ultimoDownloadArquivosInstant, Dates.ZONE_ID)) + ".CSV");
        adicionaHeaderLogFile(caminhoLog);
        return caminhoLog;
    }

    public static Path getCaminhoArquivosPerdidosDia() {
        assert ultimoDownloadArquivosInstant != null;
        Path caminhoLog = Path.of(getCaminhoArquivosLogDiarios() + File.separator + "ARQUIVOS_PERDIDOS_" + Dates.BRAZILIAN_DATE_FORMATTER_FS.format(LocalDate.ofInstant(ultimoDownloadArquivosInstant, Dates.ZONE_ID)) + ".CSV");
        adicionaHeaderLogFile(caminhoLog);
        return caminhoLog;
    }

    public static Path getCaminhoArquivosComFalhaDownloadExecucao() {
        assert ultimoDownloadArquivosInstant != null;
        Path caminhoLog = Path.of(getCaminhoArquivosLogDiarios() + File.separator + "ARQUIVOS_COM_FALHA_DOWNLOAD_" + idFilesRetrieval.get() + "_" + Dates.BRAZILIAN_DATE_TIME_FORMATTER_FS.format(LocalDateTime.ofInstant(ultimoDownloadArquivosInstant, Dates.ZONE_ID)) + ".CSV");
        adicionaHeaderLogFile(caminhoLog);
        return caminhoLog;
    }

    public static Path getCaminhoArquivosComFalhaDownloadDia() {
        assert ultimoDownloadArquivosInstant != null;
        Path caminhoLog = Path.of(getCaminhoArquivosLogDiarios() + File.separator + "ARQUIVOS_COM_FALHA_DOWNLOAD_" + Dates.BRAZILIAN_DATE_FORMATTER_FS.format(LocalDate.ofInstant(ultimoDownloadArquivosInstant, Dates.ZONE_ID)) + ".CSV");
        adicionaHeaderLogFile(caminhoLog);
        return caminhoLog;
    }

    public static Path getCaminhoArquivosComFalhaEnvioExecucao() {
        assert ultimoEnvioArquivosInstant != null;
        Path caminhoLog = Path.of(getCaminhoArquivosLogDiarios() + File.separator + "ARQUIVOS_COM_FALHA_ENVIO_" + idFilesRetrieval.get() + "_" + Dates.BRAZILIAN_DATE_TIME_FORMATTER_FS.format(LocalDateTime.ofInstant(ultimoEnvioArquivosInstant, Dates.ZONE_ID)) + ".CSV");
        adicionaHeaderLogFile(caminhoLog);
        return caminhoLog;
    }

    public static Path getCaminhoArquivosComFalhaEnvioDia() {
        assert ultimoEnvioArquivosInstant != null;
        Path caminhoLog = Path.of(getCaminhoArquivosLogDiarios() + File.separator + "ARQUIVOS_COM_FALHA_ENVIO_" + Dates.BRAZILIAN_DATE_FORMATTER_FS.format(LocalDate.ofInstant(ultimoEnvioArquivosInstant, Dates.ZONE_ID)) + ".CSV");
        adicionaHeaderLogFile(caminhoLog);
        return caminhoLog;
    }

    public static Path getCaminhoArquivosComErroExclusaoExecucao() {
        assert ultimoEnvioArquivosInstant != null;
        Path caminhoLog = Path.of(getCaminhoArquivosLogDiarios() + File.separator + "ARQUIVOS_COM_FALHA_EXCLUSAO_FTP_" + idFilesRetrieval.get() + "_" + Dates.BRAZILIAN_DATE_TIME_FORMATTER_FS.format(LocalDateTime.ofInstant(ultimoEnvioArquivosInstant, Dates.ZONE_ID)) + ".CSV");
        adicionaHeaderLogFile(caminhoLog);
        return caminhoLog;
    }

    public static Path getCaminhoArquivosComErroExclusaoDia() {
        assert ultimoEnvioArquivosInstant != null;
        Path caminhoLog = Path.of(getCaminhoArquivosLogDiarios() + File.separator + "ARQUIVOS_COM_FALHA_EXCLUSAO_FTP_" + Dates.BRAZILIAN_DATE_FORMATTER_FS.format(LocalDate.ofInstant(ultimoEnvioArquivosInstant, Dates.ZONE_ID)) + ".CSV");
        adicionaHeaderLogFile(caminhoLog);
        return caminhoLog;
    }

    private static void adicionaHeaderLogFile(Path caminhoLog) {
        int tentativas = 0;

        try {
            if (caminhoLog.toFile().exists() && Files.size(caminhoLog) != 0) return;
        } catch (Exception e) {
            logger.trace("Erro ao verificar se arquivo de log existe");
        }

        logger.trace("Criando arquivo de log " + caminhoLog);

        while (tentativas++ <= Retries.MAX_RETRY_GRAVAR_LOG) {

            try {
                Files.writeString(caminhoLog, getHeaderLogFile());
                break;
            } catch (Exception e) {
                logger.trace(String.format("Tentativa %d Erro ao gravar header do arquivo de log", tentativas));
            }

        }
    }

    /**
     * Retorna os detalhes do arquivo
     *
     * @param fileBase arquivo
     * @return
     */
    public static String getDetalhesArquivo(FileWrapper fileBase) {

        StringBuilder detalhe = new StringBuilder();
        detalhe.append(fileBase.getNomeArquivo()).append(",");
        detalhe.append(fileBase.getParent()).append(",");
        detalhe.append(fileBase.getFtpCaminhoCompleto()).append(",");

        if (fileBase.getFtpFile().getTimestamp() != null)
            detalhe.append(Dates.BRAZILIAN_DATE_TIME_FORMATTER.format(LocalDateTime.ofInstant(fileBase.getFtpFile().getTimestamp().toInstant(), Dates.ZONE_ID))).append(",");
        else
            detalhe.append(",");

        detalhe.append(fileBase.getFtpFile().getSize()).append(",");
        detalhe.append(fileBase.getFtpFile().getUser()).append(",");
        detalhe.append(fileBase.getStatusFtp().toString()).append(",");

        detalhe.append(Dates.BRAZILIAN_DATE_TIME_FORMATTER.format(LocalDateTime.ofInstant(fileBase.getFtpLatestUpdate(), Dates.ZONE_ID))).append(",");

        if (fileBase.getFindedAt() != null)
            detalhe.append(Dates.BRAZILIAN_DATE_TIME_FORMATTER.format(LocalDateTime.ofInstant(fileBase.getFindedAt(), Dates.ZONE_ID))).append(",");
        else
            detalhe.append(",");

        detalhe.append(fileBase.getStatusLocal().toString()).append(",");

        if (fileBase.getDownloadedAt() != null)
            detalhe.append(Dates.BRAZILIAN_DATE_TIME_FORMATTER.format(LocalDateTime.ofInstant(fileBase.getDownloadedAt(), Dates.ZONE_ID))).append(",");
        else
            detalhe.append(",");

        if (fileBase.getDownloadLatestUpdate() != null)
            detalhe.append(Dates.BRAZILIAN_DATE_TIME_FORMATTER.format(LocalDateTime.ofInstant(fileBase.getDownloadLatestUpdate(), Dates.ZONE_ID))).append(",");
        else
            detalhe.append(",");

        try {
            if (fileBase.getLocalCaminhoCompleto() != null && fileBase.getStatusLocal() != FileStatusLocal.INDISPONIVEL)
                detalhe.append(fileBase.getLocalCaminhoCompleto()).append(",");
            else
                detalhe.append(",");
        } catch (Exception e) {
            detalhe.append(",");
        }


        detalhe.append(fileBase.getStatusEnvio().toString()).append(",");

        if (fileBase.getEnviadoAt() != null)
            detalhe.append(Dates.BRAZILIAN_DATE_TIME_FORMATTER.format(LocalDateTime.ofInstant(fileBase.getEnviadoAt(), Dates.ZONE_ID))).append(",");
        else
            detalhe.append(",");

        if (fileBase.getEnvioLatestUpdate() != null)
            detalhe.append(Dates.BRAZILIAN_DATE_TIME_FORMATTER.format(LocalDateTime.ofInstant(fileBase.getEnvioLatestUpdate(), Dates.ZONE_ID))).append(",");
        else
            detalhe.append(",");

        try {
            if (fileBase.getCaminhoEnvioCompleto() != null && fileBase.getStatusEnvio() == FileStatusEnvio.ENVIADO)
                detalhe.append(fileBase.getCaminhoEnvioCompleto()).append(",");
            else
                detalhe.append(",");
        } catch (Exception e) {
            detalhe.append(",");
        }

        try {
            String hash = fileBase.getHash();

            if (StringUtils.isNotBlank(hash)) {
                detalhe.append(hash);
            }
        } catch (Exception e) {}

        detalhe.append(",");

        try {
            String hash = fileBase.getHashDatabase();

            if (StringUtils.isNotBlank(hash)) {
                detalhe.append(hash);
            }
        } catch (Exception e) {}

        detalhe.append(",");

        try {
            String adicional = fileBase.getAdicional();

            if (StringUtils.isNotBlank(adicional)) {
                detalhe.append(adicional);
            }
        } catch (Exception e) {}

        return detalhe.toString();
    }

    public static String getHeaderLogFile() {
        return "Nome do Arquivo,Cliente,Caminho Completo FTP,Data/Hora FTP,Tamanho em Bytes no FTP,Usuario FTP,Status FTP,Data/Hora Ultima Atualizacao FTP,Data/Hora Encontrado FTP,Status Local,Data/Hora Download Local,Data/Hora Ultima Atualizacao Download,Caminho Local,Status Envio,Data/Hora Envio,Data/Hora Ultima Atualizacao Envio,Caminho Envio,Hash SHA-1, Hash SHA-1 Database,Adicional" + System.lineSeparator();
    }

    public static String getStrArquivoSimplificado(FileWrapper fileBase) {
        return "\"" + fileBase.getNomeArquivo() + "\" ("  + fileBase.getParent() + ")";
    }

    //------------------------------------------------------------------------------------------------------------------

    // Getters e Setters

    /**
     * Retorna o datasource
     * É feito um loop até que o datasource seja instanciado
     * <p>
     * ATENÇÃO: Onde for chamado este metodo, se atente para não travar a aplicação caso o datasource não esteja instanciado ainda
     *
     * @return
     * @see #connectDatasource()
     */
    public static Datasource getDatasource() {

        // Responsavel por aguardar a finalizacao da conexao com o banco de dados
        synchronized (Datasource.isConnectingDatasource) {

            while (Datasource.isConnectingDatasource.get()) { // Enquanto a conexao com o banco de dados estiver acontecendo, aguarda
                try {
                    Datasource.isConnectingDatasource.wait();
                } catch (InterruptedException e) {
                    logger.error("Erro ao aguardar a finalizacao da thread do datasource", e);
                }
            }
        }

        return datasource;
    }

    /**
     * Retorna o usuario logado
     *
     * @return usuario
     */
    public static String getUsuario() {
        return usuario;
    }

    /**
     * Define o usuario que está executando o programa
     *
     * @param login usuario
     */
    public static void setUsuario(String login) {
        logger.info("Usuario logado: " + login);
        usuario = login;
    }

    /**
     * Retorna o ambiente em que o programa está rodando
     *
     * @return ambiente
     */
    public static String getEnvironment() {
        return Main.isProductionEnvironment ? "Produção" : "Homologação";
    }

    public static StringBuilder getRelatorioSistema() {
        Duration uptime = null;
        Duration diffTempo = null;

        try {
            RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
            uptime = Duration.ofMillis(runtimeMXBean.getUptime());
        } catch (Exception e) {
            logger.error("Erro ao obter o tempo de execução", e);
        }

        StringBuilder sb = new StringBuilder();
        sb.append(DateUtils.getCumprimento()).append(System.lineSeparator()).append(System.lineSeparator());
        sb.append("📝 Segue relatório detalhado do sistema").append(System.lineSeparator()).append(System.lineSeparator());

        sb.append("📅 Horário: ").append(Dates.BRAZILIAN_DATE_TIME_FORMATTER.format(LocalDateTime.now(Dates.ZONE_ID))).append(System.lineSeparator());

        if (uptime != null) {
            sb.append("⏱️ Tempo de execução: ").append(uptime.toHoursPart()).append(" horas e ").append(uptime.toMinutesPart()).append(" minutos").append(System.lineSeparator());
        }

        if (ultimoEnvioArquivosInstant != null) {
            diffTempo = Duration.between(ultimoEnvioArquivosInstant, Instant.now());
            sb.append("⏱️ Último envio de arquivos: ").append(diffTempo.toHoursPart()).append(" horas e ").append(diffTempo.toMinutesPart()).append(" minutos atrás").append(System.lineSeparator());
        }

        if (ultimoDownloadArquivosInstant != null) {
            diffTempo = Duration.between(ultimoDownloadArquivosInstant, Instant.now());
            sb.append("⏱️ Último download de arquivos: ").append(diffTempo.toHoursPart()).append(" horas e ").append(diffTempo.toMinutesPart()).append(" minutos atrás").append(System.lineSeparator());
        }

        if (ultimaBuscaArquivosInstant != null) {
            diffTempo = Duration.between(ultimaBuscaArquivosInstant, Instant.now());
            sb.append("⏱️ Última busca de arquivos: ").append(diffTempo.toHoursPart()).append(" horas e ").append(diffTempo.toMinutesPart()).append(" minutos atrás").append(System.lineSeparator());
        }

        sb.append(System.lineSeparator());

        try {

            // Thread para aguardar os processos sincronizados
            ProcessaUtils.createThread("Hourly Sync Wait", () -> {

                synchronized (Datasource.isConnectingDatasource) {
                    while (Datasource.isConnectingDatasource.get()) {
                        try {
                            logger.debug("Aguardando finalizar o processo de conectar ao datasource...");
                            Datasource.isConnectingDatasource.wait();
                        } catch (InterruptedException e) {
                            logger.error("Erro ao aguardar a finalizacao do processo de conectar ao datasource", e);
                        }
                    }
                }

                synchronized (MainAux.getDatasource().isRetrievingConnection) {
                    while (MainAux.getDatasource().isRetrievingConnection.get()) {
                        try {
                            logger.debug("Aguardando finalizar o processo de recuperar conexão ao datasource...");
                            MainAux.getDatasource().isRetrievingConnection.wait();
                        } catch (InterruptedException e) {
                            logger.error("Erro ao aguardar a finalizacao do processo de recuperar conexão ao datasource", e);
                        }
                    }
                }

                synchronized (MainAux.isResettingData) {
                    while (MainAux.isResettingData.get()) {
                        try {
                            logger.debug("Aguardando reset dos dados...");
                            MainAux.isResettingData.wait();
                        } catch (InterruptedException e) {
                            logger.error("Erro ao aguardar reset dos dados", e);
                        }
                    }
                }

                synchronized (MainAux.isProcurandoArquivos) {
                    while (MainAux.isProcurandoArquivos.get()) {
                        try {
                            logger.debug("Aguardando finalizar o processo de procurar arquivos...");
                            MainAux.isProcurandoArquivos.wait();
                        } catch (InterruptedException e) {
                            logger.error("Erro ao aguardar a finalizacao do processo de procurar arquivos", e);
                        }
                    }
                }

                synchronized (MainAux.isPreparingToRetrieveFilesFromFtp) {
                    while (MainAux.isPreparingToRetrieveFilesFromFtp.get()) {
                        try {
                            logger.debug("Aguardando finalizar o processo de preparar para baixar arquivos do FTP...");
                            MainAux.isPreparingToRetrieveFilesFromFtp.wait();
                        } catch (InterruptedException e) {
                            logger.error("Erro ao aguardar a finalizacao do processo de preparar para baixar arquivos do FTP", e);
                        }
                    }
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

                synchronized (MainAux.isCleaningLogs) {
                    while (MainAux.isCleaningLogs.get()) {
                        try {
                            logger.debug("Aguardando finalizar o processo de limpar logs...");
                            MainAux.isCleaningLogs.wait();
                        } catch (InterruptedException e) {
                            logger.error("Erro ao aguardar a finalizacao do processo de limpar logs", e);
                        }
                    }
                }
            }).join(Timings.WAIT_FOR_HOURLY_SYNC.toMillis()); // Tempo máximo de espera para sincronização, a fim de identificar possíveis problemas (deadlocks)

            sb.append("✅ Todos os processos finalizaram com sucesso, não há indícios de deadlocks").append(System.lineSeparator());

        } catch (InterruptedException e) {
            logger.error("Erro ao aguardar a finalizacao do processos de sincronizacao", e);

            sb.append("❌ Houve um problema ao esperar o(s) seguinte(s) processo(s) finalizarem. Verifique por possíveis deadlocks:").append(System.lineSeparator());

            if (Datasource.isConnectingDatasource.get()) {
                sb.append("  - Conectar ao datasource").append(System.lineSeparator());
            } else {
                if (MainAux.getDatasource().isRetrievingConnection.get()) {
                    sb.append("  - Recuperar conexão ao datasource").append(System.lineSeparator());
                }
            }

            if (MainAux.isResettingData.get()) {
                sb.append("  - Reset de dados").append(System.lineSeparator());
            }

            if (MainAux.isProcurandoArquivos.get()) {
                sb.append("  - Procurar arquivos").append(System.lineSeparator());
            }

            if (MainAux.isPreparingToRetrieveFilesFromFtp.get()) {
                sb.append("  - Preparar para baixar arquivos do FTP").append(System.lineSeparator());
            }

            if (MainAux.isDownloadingFilesFtp.get()) {
                sb.append("  - Baixar arquivos do FTP").append(System.lineSeparator());
            }

            if (MainAux.isSendingFilesToConnect.get()) {
                sb.append("  - Enviar arquivos para o Connect").append(System.lineSeparator());
            }

            if (MainAux.isCleaningLogs.get()) {
                sb.append("  - Limpar logs").append(System.lineSeparator());
            }

            sb.append(System.lineSeparator());
        }

        sb.append("Status do FTP: ").append(MainAux.getStatusFTP() ? "Conectado ✅" : "Desconectado ❌").append(System.lineSeparator());
        sb.append("Status de Conexão com Banco de Dados: ").append(MainAux.getStatusDatabase() ? "Conectado ✅" : "Desconectado ❌").append(System.lineSeparator()).append(System.lineSeparator());

        try {
            sb.append("📊 Total Arquivos Lista Global:").append(arquivosParaEnviar.size()).append(System.lineSeparator()).append(System.lineSeparator());

            sb.append("Estatísticas de Arquivos:").append(System.lineSeparator()).append(System.lineSeparator());

            sb.append("📊 Total Arquivos Recém Encontrados em verificação:").append(arquivosParaEnviar.stream().filter(a -> a.getStatusFtp() == FileStatusRemoto.NECESSARIO_VERIFICACAO).count()).append(System.lineSeparator());
            sb.append("📊 Total Arquivos Em Transito:").append(arquivosParaEnviar.stream().filter(a -> a.getStatusFtp() == FileStatusRemoto.EM_TRANSFERENCIA).count()).append(System.lineSeparator());
            sb.append("📊 Total Aguardando Download:").append(arquivosParaEnviar.stream().filter(a -> a.getStatusLocal() == FileStatusLocal.INDISPONIVEL && a.getStatusEnvio() != FileStatusEnvio.ENVIADO && a.getStatusFtp() == FileStatusRemoto.TRANSFERIDO).count()).append(System.lineSeparator());
            sb.append("📊 Total Disponibilizado para Envio, mas aguardando:").append(arquivosParaEnviar.stream().filter(a -> a.getStatusLocal() == FileStatusLocal.DISPONIVEL && a.getStatusEnvio() != FileStatusEnvio.ENVIADO && a.getStatusFtp() == FileStatusRemoto.TRANSFERIDO).count()).append(System.lineSeparator());
            sb.append("📊 Total Disponibilizado para Envio, mas que não serão enviados, pois possuem hash inválido:").append(arquivosParaEnviar.stream().filter(a -> a.getStatusLocal() == FileStatusLocal.DISPONIVEL && a.getStatusEnvio() != FileStatusEnvio.ENVIADO && (!a.possuiHashDatabase() || !a.comparaHashDatabase())).count()).append(System.lineSeparator());
            sb.append("📊 Total Enviados:").append(arquivosParaEnviar.stream().filter(a -> a.getStatusEnvio() == FileStatusEnvio.ENVIADO).count()).append(System.lineSeparator());
            sb.append("📊 Total Deletados:").append(arquivosParaEnviar.stream().filter(a -> a.getStatusFtp() == FileStatusRemoto.DELETADO).count()).append(System.lineSeparator());
            sb.append(System.lineSeparator()).append("Atenção: As estatísticas são baseadas na lista global. A mesma é limpa toda meia-noite.").append(System.lineSeparator());
        } catch (Exception e) {
            sb.append("Erro ao obter estatísticas de arquivos").append(System.lineSeparator());
        }


        return sb;
    }

    private static Boolean getStatusFTP() {

        try {
            if (FTPClient.getInstance() == null || !FTPClient.getInstance().isConnected()) {
                return false;
            }
        } catch (Exception e) {
            return false;
        }

        return true;
    }

    private static Boolean getStatusDatabase() {

        AtomicBoolean status = new AtomicBoolean(true);

        try {
            ProcessaUtils.createThread("Status DB", () -> {
                Connection connection = MainAux.getDatasource().getConnection();

                try {
                    if (connection == null || connection.isClosed()) {
                        status.set(false);
                    }
                } catch (SQLException e) {
                    status.set(false);
                } finally {
                    if (connection != null) {
                        try {
                            connection.close();
                        } catch (SQLException e) {}
                    }
                }

            }).join(Timings.TEMPO_LIMITE_CONEXAO_STATUS_DB);

            return status.get();
        } catch (InterruptedException e) {
            return false;
        }
    }
}
