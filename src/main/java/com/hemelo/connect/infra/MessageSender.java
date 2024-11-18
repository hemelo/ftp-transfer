/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.hemelo.connect.infra;

import com.hemelo.connect.exception.ConexaoException;
import com.hemelo.connect.utils.PropertiesUtils;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Optional;
import java.util.Properties;

/**
 * Classe que representa a conexao com o AMQP
 *
 */
public class MessageSender {

    private static final Logger logger = LoggerFactory.getLogger(MessageSender.class);

    private Connection connection;
    private Channel channel;

    private static MessageSender instance;

    private Optional<Properties> amqpProperties;

    private MessageSender() {

        try {
            amqpProperties = PropertiesUtils.getProperties("amqp.properties");

            if (amqpProperties.isEmpty()) {
                logger.debug("Não é possível conectar ao AMQP pois as propriedades não foram configuradas corretamente");
                throw new ConexaoException("Propriedades do AMQP não configuradas corretamente");
            }

            ConnectionFactory factory = new ConnectionFactory();
            factory.setVirtualHost(amqpProperties.get().getProperty("amqp.vhost"));
            factory.setHost(amqpProperties.get().getProperty("amqp.host"));
            factory.setPort(Integer.parseInt(amqpProperties.get().getProperty("amqp.port")));
            factory.setUsername(amqpProperties.get().getProperty("amqp.username"));
            factory.setPassword(amqpProperties.get().getProperty("amqp.password"));

            connection = factory.newConnection();
            channel = connection.createChannel();

            this.createExchange(amqpProperties.get().getProperty("amqp.default-exchange"), "direct", true);
        } catch (Exception ex) {
            logger.error("Erro ao conectar ao AMQP");
            throw new ConexaoException("Erro ao conectar ao AMQP", ex);
        }
    }

    public static MessageSender getInstance() {
        if (instance == null) instance = new MessageSender();
        return instance;
    }

    public Channel getChannel() {
        return channel;
    }

    public String getDefaultExchange() {
        return amqpProperties.map(p -> p.getProperty("amqp.default-exchange")).orElse(null);
    }

    /**
     * Responsável por criar exchanges
     */
    public void createExchange(String exchangeName, String type, Boolean durable) {
        try {
            channel.exchangeDeclare(exchangeName, type, durable);
            logger.debug("Exchange " + exchangeName + " criada com sucesso.");
        } catch (IOException ex) {
            logger.error("Erro ao criar exchange AMQP", ex);
        }
    }

    /**
     * Responsável por criar bindings
     */
    public void bindQueue(String queueName, String exchangeName, Collection<String> routingKeys) {
        try {
            channel.queueDeclare(queueName, true, false, false, null);

            for (String rk : routingKeys) {
                channel.queueBind(queueName, exchangeName, rk);
                logger.debug("Rotas " + routingKeys + " adicionadas para queue " + queueName + " em " + exchangeName);
            }

        } catch (Exception ex) {
            logger.error("Erro ao criar queue AMQP", ex);
        }
    }

    /**
     * Responsável por enviar mensagens
     */
    public void send(String routingKey, String exchangeName, String message) {
        try {
            getChannel().basicPublish(exchangeName, routingKey, null, message.getBytes());
            logger.debug("Mensagem enviada em " + exchangeName + ", rota " + routingKey);
        } catch (IOException ex) {
            logger.error("Erro ao enviar mensagem via AMQP", ex);
        }
    }

    /**
     * Responsável por fechar a conexão AMQP
     */
    public void close() {
        try {
            channel.close();
            connection.close();
        } catch (Exception ex) {
            logger.error("Erro ao fechar conexão AMQP", ex);
        }
    }
}
