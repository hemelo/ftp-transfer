package com.hemelo.connect.constants;

import com.hemelo.connect.Main;
import com.hemelo.connect.utils.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.Set;

/**
 * Intervalos de tempo utilizados no sistema
 */
public class Timings {

    public static final Logger logger = LoggerFactory.getLogger(Timings.class);

    // Intervalo responsável por controlar a frequência de verificação de arquivos no FTP
    public static final Duration INTERVALO_PESQUISA_ARQUIVOS = Duration.ofMinutes(Main.isProductionEnvironment ? 20 : 1);

    // Intervalo responsável por controlar a frequência de verificação de arquivos no FTP antes de uma nova tentativa durante o download de arquivos
    // Esse intervalo é utilizado quando tem algum arquivo em transferência e o sistema tá executando a funcionalidade de baixar arquivos
    // A fim de não interromper o download, o programa espera o arquivo ser enviado para o FTP completamente antes de tentar baixar novamente
    public static final Long INTERVALO_PESQUISA_ARQUIVOS_BEFORE_RETRIEVAL_MS = Duration.ofMinutes(Main.isProductionEnvironment ? 2 : 1).toMillis();

    // Intervalo responsável por controlar a frequência de verificação de arquivos locais para envio ao Connect
    // Esse intervalo é utilizado para verificar se tem arquivos disponíveis para envio ao Connect
    // Os arquivos ficam disponíveis para envio ao Connect quando são transferidos do FTP para a máquina local
    public static final Long INTERVALO_PESQUISA_ARQUIVOS_LOCAIS_ENVIO_CONNECT_MS = Duration.ofMinutes(2).toMillis();

    // Intervalo responsável por definir o tempo de espera entre tentativas de operações IO
    public static final Long INTERVALO_ERRO_IO_RETRY = Duration.ofSeconds(1).toMillis();

    // Define os horários em que o sistema deve baixar os arquivos do FTP para a máquina local
    public static final Set<LocalTime> HORARIOS_DOWNLOAD_ARQUIVOS_FTP = Main.isProductionEnvironment
            ? Set.of(LocalTime.of(10, 0), LocalTime.of(12, 0), LocalTime.of(14, 0), LocalTime.of(16, 0), LocalTime.of(18, 0), LocalTime.of(20, 0), LocalTime.of(22, 0))
            : DateUtils.generateHoursFromInterval(LocalTime.of(0, 0), LocalTime.of(23, 59, 59), 1460);

    // Define o tempo de espera após o download dos arquivos do FTP para a máquina local
    public static final Long DELAY_AFTER_DOWNLOAD_FILES = Main.isProductionEnvironment ? 0L : 5000L;

    public static final Duration TEMPO_MINIMO_DOWNLOAD = Duration.ofMinutes(1);

    // Define o tempo limite para download de arquivos
    public static final Duration TEMPO_LIMITE_DOWNLOAD = Duration.ofMinutes(5);

    // Define o tempo limite para upload de arquivos
    public static final Duration WAIT_FOR_HOURLY_SYNC = Duration.ofMinutes(10);

    // Define o tempo utilizado para tentar novamente a conexão com o socket server
    public static final long TEMPO_RETRY_SOCKET_SERVER = Duration.ofMinutes(1).toMillis();

    // Define o tempo utilizado para tentar novamente se conectar ao socket server
    public static final long TEMPO_RETRY_SOCKET_CLIENT = Duration.ofSeconds(10).toMillis();

    // Define o tempo utilizado para tentar novamente abrir Stream de IO no socket
    public static final long TEMPO_RETRY_SOCKET_IO = Duration.ofSeconds(5).toMillis();

    // Define o tempo utilizado para tentar novamente abrir Stream de IO no socket
    public static final long TEMPO_MAXIMO_RELATORIO = Duration.ofMinutes(5).toMillis();

    // Define o tempo mínimo entre downloads
    public static final long INTERVALO_MINIMO_ENTRE_DOWNLOADS_MINUTOS =  Duration.ofMinutes(Main.isProductionEnvironment ? 5 : 1).toMinutes();

    // Define o tempo limite para conexão com o banco de dados para validar status
    public static final long TEMPO_LIMITE_CONEXAO_STATUS_DB = Duration.ofSeconds(30).toMillis();
}
