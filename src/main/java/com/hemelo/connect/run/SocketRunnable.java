package com.hemelo.connect.run;

import com.hemelo.connect.MainAux;
import com.hemelo.connect.constants.Timings;
import com.hemelo.connect.enums.TopicEmail;
import com.hemelo.connect.infra.Mailer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;

public class SocketRunnable implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(SocketRunnable.class);
    private static final int port = 31321;

    private static ServerSocket serverSocket = null;
    public void run() {

        Socket clientSocket = null;
        PrintWriter out = null;
        BufferedReader in = null;
        int tentativas = 0;

        try {

            while (serverSocket == null || serverSocket.isClosed()) {
                try {
                    serverSocket = new ServerSocket(port);
                    logger.info("Listening on port " + port);
                } catch (IOException e) {

                    if (tentativas == 0) {
                        logger.error("Could not listen on port " + port);
                    } else {
                        logger.error("Could not listen on port " + port + " - Attempt " + tentativas);
                    }

                    tentativas++;

                    try {
                        Thread.sleep(Timings.TEMPO_RETRY_SOCKET_SERVER);
                    } catch (InterruptedException ex) {
                        logger.error("Thread interrupted");
                        throw ex;
                    }

                }
            }

            tentativas = 0;

            while (clientSocket == null || clientSocket.isClosed()) {
                try {
                    clientSocket = serverSocket.accept();
                    logger.info("Accepted connection from " + clientSocket.getLocalAddress());
                } catch (IOException e) {
                    if (tentativas == 0) {
                        logger.error("Accept failed: " + port);
                    } else {
                        logger.error("Accept failed: " + port + " - Attempt " + tentativas);
                    }

                    tentativas++;

                    try {
                        Thread.sleep(Timings.TEMPO_RETRY_SOCKET_SERVER);
                    } catch (InterruptedException ex) {
                        logger.error("Thread interrupted");
                        throw ex;
                    }
                }
            }

            tentativas = 0;

            while (out == null) {
                try {
                    out = new PrintWriter(clientSocket.getOutputStream(), true);
                } catch (IOException e) {
                    if (tentativas == 0) {
                        logger.error("Error while creating PrintWriter");
                    } else {
                        logger.error("Error while creating PrintWriter - Attempt " + tentativas);
                    }

                    tentativas++;

                    try {
                        Thread.sleep(Timings.TEMPO_RETRY_SOCKET_IO);
                    } catch (InterruptedException ex) {
                        logger.error("Thread interrupted");
                        throw ex;
                    }
                }
            }

            tentativas = 0;

            while (in == null) {
                try {
                    in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                } catch (IOException e) {
                    if (tentativas == 0) {
                        logger.error("Error while creating BufferedReader");
                    } else {
                        logger.error("Error while creating BufferedReader - Attempt " + tentativas);
                    }

                    tentativas++;

                    try {
                        Thread.sleep(Timings.TEMPO_RETRY_SOCKET_IO);
                    } catch (InterruptedException ex) {
                        logger.error("Thread interrupted");
                        throw ex;
                    }
                }
            }

            String inputLine;

            logger.info("Reading input from client");

            try {
                while ((inputLine = in.readLine()) != null && !inputLine.isEmpty()) {

                    String[] parts = inputLine.split(" ");
                    String command = parts[0];
                    String subcommand = parts.length > 1 ? parts[1] : "";
                    StringBuilder sb = new StringBuilder();

                    logger.debug("Received command: " + command);

                    switch (command) {
                        case "PING":
                            sb.append("PONG").append(System.lineSeparator());
                            break;
                        case "HEARTBEAT":
                            sb.append("HEARTBEAT").append(System.lineSeparator());
                            break;
                        case "TESTMAIL":

                            if (parts.length < 2) {
                                sb.append("Por favor, informe o email de destino").append(System.lineSeparator());
                                break;
                            }

                            sb.append("Disparo de email de teste para " + parts[1] + "efetuado").append(System.lineSeparator());

                            List<String> destinatarios = new ArrayList<>();
                            destinatarios.add(parts[1]);
                            Mailer.getInstance().sendMail(TopicEmail.TESTE, "Teste de email", null, destinatarios);
                            break;
                        case "RELATORIO":

                            sb.append("Gerando relatório...").append(System.lineSeparator());
                            sb.append("Por favor, aguarde...").append(System.lineSeparator());
                            sb.append("Esse comando pode demorar um pouco e irá falhar se passar de um tempo limite...").append(System.lineSeparator());

                            Thread relatorioThread = new Thread(() -> {
                                StringBuilder sb2 = MainAux.getRelatorioSistema();

                                if (sb2.isEmpty()) {
                                    sb.append("❌ Não foi possível gerar relatório").append(System.lineSeparator());;
                                } else {
                                    sb.append(sb2).append(System.lineSeparator());
                                }
                            });

                            relatorioThread.start();

                            try {
                                relatorioThread.join(Timings.TEMPO_MAXIMO_RELATORIO);
                            } catch (InterruptedException e) {
                                sb.append("❌ Erro ao gerar relatório. Tempo limite excedido. Tente novamente e/ou erifique se o programa está funcionando de fato.\r\n");
                            }

                            break;
                        default:
                            sb.append(inputLine).append(System.lineSeparator());
                            break;
                    }

                    sb.append("EOF");
                    out.println(sb);
                    out.flush();
                }
            } catch (IOException e) {

                if (e instanceof SocketException && e.getMessage().equals("Connection reset")) {
                    logger.debug("Connection reset by client");
                } else {
                    logger.error("Error while reading input from client", e);
                }
            }
        } catch (Exception ex) {
            logger.error("Error while running SocketRunnable", ex);
        } finally {

            if (out != null) {
                try {
                    out.close();
                } catch (Exception e) {
                    logger.error("Error while closing PrintWriter", e);
                }
            }

            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    logger.error("Error while closing BufferedReader", e);
                }
            }

            if (clientSocket != null) {
                try {
                    clientSocket.close();
                } catch (IOException e) {
                    logger.error("Error while closing clientSocket", e);
                }
            }

            if (Thread.currentThread().isInterrupted()) {
                logger.info("Thread interrupted");
            }
        }
    }
}
