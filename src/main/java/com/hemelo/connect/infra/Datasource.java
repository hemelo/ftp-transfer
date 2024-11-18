/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.hemelo.connect.infra;

import com.hemelo.connect.exception.ConexaoException;
import com.hemelo.connect.utils.PropertiesUtils;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * Classe que contém o datasource do banco de dados
 * Utiliza o pool de conexões HikariCP
 */
public final class Datasource {

    private static final Logger logger = LoggerFactory.getLogger(Datasource.class);

    private final boolean production;
    public static final AtomicBoolean isConnectingDatasource;
    public final AtomicBoolean isRetrievingConnection;

    private HikariConfig config;
    private HikariDataSource ds;
    private Properties properties;

    static {
        isConnectingDatasource = new AtomicBoolean(false);
    }
    /**
     * Construtor da classe
     * Cria a conexao com o banco de dados e inicializa o pool de conexões
     * Se nao conseguir conectar ao banco de dados, tenta novamente por 5 vezes
     *
     * @param production quando true, conecta ao banco de produção, quando false, conecta ao banco de homologação
     * @throws ConexaoException
     */
    public Datasource(boolean production) throws ConexaoException {

        this.production = production;
        isRetrievingConnection = new AtomicBoolean(false);

        synchronized (isConnectingDatasource) {
            isConnectingDatasource.set(true);
            isConnectingDatasource.notifyAll();

            var propertiesOpt = PropertiesUtils.getProperties("database.properties");

            if (propertiesOpt.isEmpty()) {
                isConnectingDatasource.set(false);
                isConnectingDatasource.notifyAll();
                logger.debug("Arquivo de propriedades de banco de dados não foi encontrado.");
                throw new ConexaoException("Erro ao conectar ao banco de dados. Arquivo de propriedades não foi encontrado.");
            } else {

                properties = new Properties();

                if (production) {
                    properties.setProperty("host", propertiesOpt.get().getProperty("db.producao.ip"));
                    properties.setProperty("port", propertiesOpt.get().getProperty("db.producao.port"));
                    properties.setProperty("database", propertiesOpt.get().getProperty("db.producao.database"));
                    properties.setProperty("user", propertiesOpt.get().getProperty("db.producao.user"));
                    properties.setProperty("password", propertiesOpt.get().getProperty("db.producao.password"));
                } else {
                    properties.setProperty("host", propertiesOpt.get().getProperty("db.homologacao.ip"));
                    properties.setProperty("port", propertiesOpt.get().getProperty("db.homologacao.port"));
                    properties.setProperty("database", propertiesOpt.get().getProperty("db.homologacao.database"));
                    properties.setProperty("user", propertiesOpt.get().getProperty("db.homologacao.user"));
                    properties.setProperty("password", propertiesOpt.get().getProperty("db.homologacao.password"));
                }
            }

            final String ambiente = production ? "produção" : "homologação";

            logger.debug("Conectando ao banco de " + ambiente + " [" + properties.get("host") + ":" + properties.get("port") + "]");

            int tentativas = 0;

            Function<Integer, Integer> timeoutCalculator = (Integer t) -> switch (t) {
                case 1 -> 100;
                case 2 -> 200;
                case 3 -> 500;
                case 4 -> 1000;
                case 5 -> 3000;
                default -> throw new IllegalStateException("Unexpected value: " + t);
            };

            config = new HikariConfig();

            while (ds == null && ++tentativas <= 5) {
                try {
                    Class.forName("com.mysql.cj.jdbc.Driver");

                    config.setJdbcUrl(getConnectionString());
                    config.setUsername(properties.getProperty("user"));
                    config.setPassword(properties.getProperty("password"));
                    config.addDataSourceProperty("cachePrepStmts", "true");
                    config.addDataSourceProperty("prepStmtCacheSize", "250");
                    config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");

                    ds = new HikariDataSource(config);

                } catch (ClassNotFoundException ex) {
                    logger.debug("Classe de conexão com o banco de dados não encontrada.");
                    isConnectingDatasource.set(false);
                    isConnectingDatasource.notifyAll();
                    throw new ConexaoException("Erro ao conectar ao sistema.", ex);
                } catch (Exception ex) {
                    logger.debug(String.format("Erro ao conectar ao banco de dados. Tentativa %s", tentativas), ex);
                }

                try {
                    Thread.sleep(timeoutCalculator.apply(tentativas));
                } catch (Exception ex) {
                    logger.debug("Erro ao tentar esperar para conectar novamente ao banco de dados", ex);
                }
            }

            if (ds == null) {
                logger.error("Erro ao conectar ao banco de dados. Número maximo de tentativas atingido.");
                throw new ConexaoException("Erro ao conectar ao banco de dados");
            } else {
                logger.debug(String.format("Pool de conexoes com o banco de dados de %s estabelecido", ambiente));
            }


            isConnectingDatasource.set(false);
            isConnectingDatasource.notifyAll();
        }
    }

    /**
     * Recupera uma conexao com o banco de dados
     * Se nao conseguir conectar ao banco de dados, tenta novamente por 5 vezes
     *
     * @throws ConexaoException
     */
    public Connection getConnection() {

        AtomicReference<Connection> connection = new AtomicReference<>();

        Function<Integer, Integer> timeoutCalculator = (Integer t) -> switch (t) {
            case 1 -> 1000;
            case 2 -> 2000;
            case 3 -> 5000;
            case 4 -> 10000;
            case 5 -> 30000;
            default -> throw new IllegalStateException("Unexpected value: " + t);
        };

        synchronized (isRetrievingConnection) {

            while (isRetrievingConnection.get()) {
                try {
                    isRetrievingConnection.wait();
                } catch (InterruptedException ex) {
                    logger.error("Erro ao aguardar a finalizacao da thread de conexao", ex);
                }
            }

            isRetrievingConnection.set(true);
            isRetrievingConnection.notifyAll();

            Integer tentativas = 0;

            while (connection.get() == null && ++tentativas <= 5) {
                try {
                    connection.set(ds.getConnection());
                } catch (SQLException ex) {
                    logger.error("Erro ao conectar ao sistema. Tentativa " + tentativas, ex);
                }

                try {
                    Thread.sleep(timeoutCalculator.apply(tentativas));
                } catch (Exception ex) {
                    logger.debug("Erro ao tentar esperar para conectar novamente ao sistema", ex);
                }
            }

            isRetrievingConnection.set(false);
            isRetrievingConnection.notifyAll();

            if (connection.get() == null) {
                logger.error("Erro ao conectar ao banco de dados. Número maximo de tentativas atingido.");
                throw new ConexaoException("Erro ao conectar ao banco de dados");
            }

            while (isRetrievingConnection.get()) {
                try {
                    isRetrievingConnection.wait();
                } catch (InterruptedException ex) {
                    logger.error("Erro ao aguardar a finalizacao da thread de conexao", ex);
                }
            }

            return connection.get();
        }
    }

    public void close() {

        if (ds != null)
            ds.close();
    }

    /**
     * Retorna o IP do banco de dados
     * @return IP do banco de dados
     */
    public String getIp() {
        if (properties != null)
            return properties.getProperty("host");

        return null;
    }

    /**
     * Retorna se a conexao é com o banco de producao
     * @return true se for producao, false se for homologacao
     */
    public Boolean isProduction() {
        return production;
    }

    /**
     * Retorna a string de conexao com o banco de dados
     * @return string de conexao
     */
    public String getConnectionString() {
        if (properties != null)
            return "jdbc:mysql://" + properties.get("host") + ":" + properties.get("port") + "/" + properties.get("database") + "?zeroDateTimeBehavior=convertToNull&useSSL=false";

        return null;
    }
}
