package com.hemelo.connect.infra;

import com.hemelo.connect.Main;
import com.hemelo.connect.MainAux;
import com.hemelo.connect.dao.GenericDao;
import com.hemelo.connect.enums.TopicEmail;
import com.hemelo.connect.exception.ConexaoException;
import com.hemelo.connect.utils.ProcessaUtils;
import com.hemelo.connect.utils.PropertiesUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.mail2.core.EmailException;
import org.apache.commons.mail2.javax.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class Mailer {

    private static final Logger logger = LoggerFactory.getLogger(Mailer.class);

    private static Optional<Properties> properties;
    private static Mailer instance;
    private final AtomicBoolean isSendingMail;
    private GenericDao genericDao;

    private Mailer() {
        properties = PropertiesUtils.getProperties("mail.properties");
        isSendingMail = new AtomicBoolean(false);
    }

    public static Mailer getInstance() {
        if (instance == null) instance = new Mailer();
        return instance;
    }

    /**
     * Envia um email
     * @param topic topic do email
     * @param body corpo do email
     * @return Thread
     */
    public Thread sendMail(TopicEmail topic, String body) {
        return this.sendMail(topic, body, null, null);
    }

    /**
     * Envia um email
     * @param topic topic do email
     * @param body corpo do email
     * @param anexos lista de arquivos anexos
     * @return Thread
     */
    public Thread sendMail(TopicEmail topic, String body, List<File> anexos) {
        return this.sendMail(topic, body, anexos, null);
    }

    /**
     * Envia um email
     * @param topic topic do email
     * @param body corpo do email
     * @param anexos lista de arquivos anexos
     * @param destinatarios lista de destinatarios
     * @return Thread
     */
    public Thread sendMail(TopicEmail topic, String body, List<File> anexos, List<String> destinatarios) {
        // Enviar email assincrono
        return ProcessaUtils.createThread("Mailer", () -> {

            if (properties.isEmpty()) {
                logger.error("Não é possível enviar o email pois as propriedades não foram configuradas corretamente");
                return;
            }

            if (!Main.enviarEmails) {
                logger.info("Envio de e-mails desativado");
                return;
            }

            // Aguarda para enviar o email se outro email estiver sendo enviado
            synchronized (isSendingMail) {
                while (isSendingMail.get()) {
                    try {
                        logger.trace("Aguardando para enviar e-mail");
                        isSendingMail.wait();
                    } catch (InterruptedException e) {
                        logger.error("Erro ao esperar para enviar e-mail", e);
                    }
                }

                isSendingMail.set(true);
                isSendingMail.notifyAll();
            }

            if (genericDao == null) {
                genericDao = new GenericDao(MainAux.getDatasource());
            }

            List<String> _destinatarios = null;

            if (destinatarios == null || destinatarios.isEmpty()) {
                _destinatarios = Main.isProductionEnvironment
                        ? genericDao.buscarDestinatariosEmail(topic)
                        : Arrays.asList(Objects.requireNonNullElse(properties.get().get("mail.destinatarios.seds"), "").toString().split(","));
            } else {
                _destinatarios = destinatarios;
            }

            if (_destinatarios == null || _destinatarios.isEmpty()) {
                logger.error("Não foi possível enviar o email, pois não há destinatários cadastrados para o topic " + topic.getSubscriptionTopic());

                synchronized (isSendingMail) {
                    isSendingMail.set(false);
                    isSendingMail.notifyAll();
                }

                return;
            }

            String _body = body.replaceAll("\n", "<br>").replaceAll("\\\\", "/");

            try {
                MultiPartEmail  email = getEmailBase();

                email.setSubject(topic.toString());
                email.setContent(StringEscapeUtils.unescapeJava(_body), "text/html; charset=UTF-8");
                email.addTo(_destinatarios.toArray(new String[0]));

                if (anexos != null && !anexos.isEmpty()) {
                    for (File anexo : anexos) {
                        email.attach(anexo);
                    }
                }

                email.send();

                logger.info("Email enviado para " + _destinatarios.size() + " destinatários referente ao tópico " + topic.getSubscriptionTopic());

            } catch (Exception e) {
                logger.error("Erro ao enviar email", e);
            }

            synchronized (isSendingMail) {
                isSendingMail.set(false);
                isSendingMail.notifyAll();
            }
        });
    }

    /**
     * Retorna uma instância de Email configurada com as propriedades do arquivo mail.properties
     * @return
     */
    private static MultiPartEmail  getEmailBase() throws EmailException {

        if (properties.isEmpty()) {
            throw new ConexaoException("Propriedades do email não configuradas corretamente");
        }

        MultiPartEmail email = new MultiPartEmail();
        email.setHostName(properties.get().getProperty("mail.host"));
        email.setFrom(properties.get().getProperty("mail.from"), properties.get().getProperty("mail.from.name"));
        email.setSmtpPort(Integer.parseInt(properties.get().getProperty("mail.port")));
        email.setAuthentication(properties.get().getProperty("mail.user"), properties.get().getProperty("mail.password"));
        email.setStartTLSEnabled(true);
        return email;
    }
}
