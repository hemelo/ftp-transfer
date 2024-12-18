package com.hemelo.connect;

import com.hemelo.connect.constants.Timings;
import com.hemelo.connect.run.*;
import com.hemelo.connect.utils.ProcessaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * Classe principal da aplicação
 * O programa consiste na execução de múltiplas threads
 * Cada thread é responsável por uma tarefa específica
 * As threads são executadas em intervalos de tempo específicos
 * As threads são responsáveis por verificar novos arquivos no FTP, baixar arquivos do FTP, enviar arquivos para o Connect, entre outras tarefas
 * As threads são executadas em paralelo
 * Para garantir a integridade dos dados, as threads são sincronizadas
 * Para melhor entendimento, as funções executadas nessas threads estão separadas no pacote {@link com.hemelo.connect.run}
 * As threads são criadas na função {@link Main#main(String[])}
 */
public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    // Variavel que indica se a aplicacao esta em ambiente de producao
    // Cuidado ao alterar essa variavel, pois ela é utilizada para definir o ambiente de producao
    // O banco de dados de producao é diferente do banco de dados de desenvolvimento
    public static final boolean isProductionEnvironment = false;

    // Variavel que indica se a aplicacao deve validar o hash dos arquivos baixados
    public static final boolean validaHashArquivoDb = Main.isProductionEnvironment;

    // Variavel que indica se a aplicacao deve enviar emails
    public static final boolean enviarEmails = true;

    public static void main(String[] args) {
        MainAux.imprimeConfiguracoes();
        MainAux.createSystemTray();
        MainAux.validaProperties();
        MainAux.connectDatasource();
        MainAux.waitForDatasource();

        // Thread para ser executada a cada minuto
        MainAux.minuteTimer = ProcessaUtils.createTimer("por Minuto", Duration.ofMinutes(1), new MinuteRunnable());

        // Thread para ser executada a cada dia
        MainAux.dailyTimer = ProcessaUtils.createTimer("Diário", Duration.ofDays(1), new DailyRunnable());

        // Thread para ser executada a cada hora
        MainAux.hourlyTimer = ProcessaUtils.createTimer("por Hora", Duration.ofHours(1), new HourRunnable());

        // Timer para verificar novos arquivos no FTP
        MainAux.arquivosFinderTimer = ProcessaUtils.createTimer("Arquivos Finder", Timings.INTERVALO_PESQUISA_ARQUIVOS, new ArquivosFtpFinderRunnable());

        // Timer para baixar os arquivos do FTP
        MainAux.arquivosDownloaderThread = ProcessaUtils.createLoopedThread("Arquivos Downloader", new ArquivosDownloaderRunnable());

        // Thread para mover arquivos baixados locais do FTP para as devidas pastas do Connect.
        MainAux.arquivosSenderTimer = ProcessaUtils.createTimer("Arquivos Sender", Duration.ofMillis(Timings.INTERVALO_PESQUISA_ARQUIVOS_LOCAIS_ENVIO_CONNECT_MS), new ArquivosSenderRunnable());

        // Thread do socket
        MainAux.serverThread = ProcessaUtils.createLoopedThread("Socket", new SocketRunnable());
    }
}
