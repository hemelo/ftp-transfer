package com.hemelo.connect.infra;

import com.hemelo.connect.MainAux;
import com.hemelo.connect.constants.Caminhos;
import com.hemelo.connect.constants.Timings;
import com.hemelo.connect.dao.GenericDao;
import com.hemelo.connect.exception.ConexaoException;
import com.hemelo.connect.utils.PropertiesUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.net.ProtocolCommandEvent;
import org.apache.commons.net.ProtocolCommandListener;
import org.apache.commons.net.ftp.FTP;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketException;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

/**
 * Classe gateway para conexão com o servidor FTP
 * É responsável por criar uma instância do cliente FTP real do Apache Commons Net
 * e retornar essa instância para ser utilizada em outras classes
 */
public final class FTPClient {

    private static final Logger logger = LoggerFactory.getLogger(FTPClient.class);
    private static Optional<Properties> props;

    private static FTPClient instance;
    private org.apache.commons.net.ftp.FTPClient ftpClient;

    private FTPClient() {
        loadFtpConfig();

        if (props.isPresent() && (!props.get().containsKey("ftp.password") || StringUtils.isBlank(props.get().getProperty("ftp.password")))) {
            buscarSenhaFtp();
        }
    }

    /**
     * Retorna uma instância do cliente FTP real do Apache Commons Net
     * @return
     * @throws Exception
     */
    public static org.apache.commons.net.ftp.FTPClient getInstance() throws Exception {
        if (instance == null) {
            instance = new FTPClient();
        }

        if (instance.ftpClient == null || !instance.ftpClient.isConnected()) {

            if (props.isEmpty()) {
                return null;
            }

            instance.ftpClient = getClientFtp(
                    props.get().getProperty("ftp.host"),
                    Integer.parseInt(props.get().getProperty("ftp.port")),
                    props.get().getProperty("ftp.user"),
                    props.get().getProperty("ftp.password")
            );

            instance.ftpClient.addProtocolCommandListener(new ProtocolCommandListener() {
                @Override
                public void protocolCommandSent(ProtocolCommandEvent protocolCommandEvent) {
                    logger.debug(String.format("Command sent: [%s]-%s",
                            protocolCommandEvent.getCommand(),
                            Optional.ofNullable(protocolCommandEvent.getMessage()).map(s -> s.replace("\n", "")).orElse("")));
                }

                @Override
                public void protocolReplyReceived(ProtocolCommandEvent protocolCommandEvent) {
                    String message = Optional.ofNullable(protocolCommandEvent.getMessage()).map(s -> s.replace("\n", "")).orElse("");

                    if (!message.isBlank())
                        logger.debug(String.format("Reply received : %s", message));
                }
            });
        }

        return Objects.requireNonNull(instance.ftpClient, "Não foi possível conectar ao FTP");
    }

    /**
     * Carrega configurações do FTP
     */
    private static void loadFtpConfig() {
        props = PropertiesUtils.getProperties("ftp.properties");

        if (props.isEmpty()) {
            logger.debug("Não será possível autenticar usuários pois as propriedades do LDAP não foram configuradas corretamente");
        }
    }

    /**
     * Retorna um cliente FTP conectado ao servidor FTP
     */
    private static void buscarSenhaFtp() {

        if (props.isEmpty()) return;

        GenericDao dao = new GenericDao(MainAux.getDatasource());
        String usuario = props.get().getProperty("ftp.user");

        logger.debug("Buscando senha do FTP para usuario " + usuario);

        try {
            props.get().setProperty("ftp.password", dao.buscarSenhaFtp(usuario));
        } catch (Exception e) {
            logger.error("Erro ao buscar senha do FTP para usuario " + usuario, e);
        }
    }

    /**
     * Cria uma instância do cliente FTP real do Apache Commons Net
     * @param host
     * @param port
     * @param username
     * @param password
     * @return
     * @throws Exception
     */
    private static org.apache.commons.net.ftp.FTPClient getClientFtp(String host, int port, String username, String password) throws Exception {
        try {
            org.apache.commons.net.ftp.FTPClient ftpClient = new org.apache.commons.net.ftp.FTPClient();

            logger.info("Conectando ao FTP " + host + ":" + port + " com usuário " + username);


            ftpClient.connect(host, port);
            ftpClient.setDataTimeout(Timings.TEMPO_LIMITE_DOWNLOAD);
            ftpClient.setSoTimeout((int) Timings.TEMPO_LIMITE_DOWNLOAD.toMillis());
            ftpClient.setAutodetectUTF8(true);
            ftpClient.setBufferSize(0);
            ftpClient.setControlKeepAliveTimeout(300);
            ftpClient.login(username, password);
            ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
            ftpClient.enterLocalPassiveMode();
            return ftpClient;
        } catch (SocketException e) {
            logger.error("Erro ao conectar ao FTP", e);
            throw new ConexaoException("Erro ao conectar ao FTP");
        }
    }

    /**
     * Move para o diretório de trabalho
     * @return
     */
    public static boolean moveToWorkBaseDirectory() {
        return changeWorkingDirectory(Caminhos.CAMINHO_BASE_FTP);
    }


    /**
     * Move para o diretório de trabalho informado
     * @param caminho - Caminho para onde se deseja mover
     * @return
     */
    public static boolean changeWorkingDirectory(String caminho) {

        try {
            if (!Objects.requireNonNullElse(getInstance().printWorkingDirectory(), "").equals(caminho) && !getInstance().changeWorkingDirectory(caminho)) {
                logger.error(String.format("Erro ao mudar de diretorio para %s", caminho));
                return false; // Se não conseguir mudar de diretorio, retorna false
            } else return true;
        } catch (SocketException e) {
            instance.ftpClient = null;
            logger.error("Erro ao mudar de diretorio para " + caminho, e);
            return false;
        } catch (Exception e) {
            logger.error("Erro ao mudar de diretorio para " + caminho, e);
            return false;
        }
    }
}
