package com.hemelo.connect.dao;


import com.hemelo.connect.enums.TopicEmail;
import com.hemelo.connect.infra.Datasource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Classe responsavel por realizar consultas genéricas e auxiliares no banco de dados
 */
public final class GenericDao {

    private Datasource datasource;

    private static final Logger logger = LoggerFactory.getLogger(GenericDao.class);

    public GenericDao(Datasource datasource) {
        this.datasource = datasource;
    }

    /**
     * Busca a senha de um usuário no banco de dados
     * @param usuario Nome do usuário
     * @return Senha do usuário
     * @throws SQLException Em caso de erro na consulta
     */
    public String buscarSenhaFtp(String usuario) throws SQLException {
        String senha = null;
        String query = getQueryConsultaSenhaFtp();

        Connection conn = datasource.getConnection();
        PreparedStatement stmt = conn.prepareStatement(query);
        stmt.setString(1, usuario);
        ResultSet rs = stmt.executeQuery();

        if (rs.next()) {
            senha = rs.getString("senha");
        }

        return senha;
    }

    /**
     * Busca os destinatários que deverão receber um email de acordo com o tópico
     * @param topic Tópico do email
     * @return Lista de destinatários
     */
    public List<String> buscarDestinatariosEmail(TopicEmail topic) {

        String query = getQueryConsultaDestinatariosEmail();

        try (Connection conn = datasource.getConnection()) {

            PreparedStatement stmt = conn.prepareStatement(query);
            stmt.setString(1, topic.getSubscriptionTopic());

            ResultSet rs = stmt.executeQuery();

            List<String> destinatarios = new ArrayList<>();

            while (rs.next()) {
                destinatarios.add(rs.getString("email"));
            }

            return destinatarios;
        } catch (SQLException e) {
            logger.error("Erro ao buscar destinatários de email", e);
            return new ArrayList<>();
        }
    }

    public void inscreverDestinatarioEmail(String email, TopicEmail topic) {
        String query = getQueryInscricaoDestinatarioEmail();

        try (Connection conn = datasource.getConnection()) {
            PreparedStatement stmt = conn.prepareStatement(query);
            stmt.setString(1, email);
            stmt.setString(2, topic.getSubscriptionTopic());
            stmt.executeUpdate();
        } catch (SQLException e) {
            logger.error("Erro ao inscrever destinatário de email", e);
        }
    }

    //------------------------------------------------------------------------------------------------------------------

    private String getQueryConsultaSenhaFtp() {
        return "SELECT AES_DECRYPT(senha,'geracao') as senha FROM controle_cofre.usuario WHERE login = ?";
    }

    private String getQueryConsultaDestinatariosEmail() {
        return "SELECT email FROM telegram.Subscription WHERE topic = ?";
    }

    private String getQueryInscricaoDestinatarioEmail() {
        return "INSERT INTO telegram.Subscription (email, topic) VALUES (?, ?)";
    }
}
