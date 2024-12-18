package com.hemelo.connect.dao;

import com.hemelo.connect.dto.FileWrapper;
import com.hemelo.connect.enums.FileStatusDatabase;
import com.hemelo.connect.infra.Datasource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public final class TransitoDao {

    private Datasource datasource;

    private static final Logger logger = LoggerFactory.getLogger(TransitoDao.class);

    public TransitoDao(Datasource datasource) {
        this.datasource = datasource;
    }

    /**
     * Atualiza o arquivo com os dados do banco de dados
     * @param fileWrapper Arquivo a ser atualizado
     * @return Arquivo atualizado
     * @throws SQLException Em caso de erro na consulta
     */
    public FileWrapper atualizaArquivoComDadosDoBd(FileWrapper fileWrapper) throws SQLException {
        String query = getQueryConsultaArquivoFtp();

        try (Connection conn = datasource.getConnection()) {
            PreparedStatement stmt = conn.prepareStatement(query);
            stmt.setString(1, fileWrapper.getNomeArquivo());
            ResultSet rs = stmt.executeQuery();

            if (rs.next()) {

                String status = rs.getString("status");
                String hash = rs.getString("hash");

                fileWrapper.setHashDatabase(hash);

                logger.trace(String.format("Arquivo " + fileWrapper.getNomeArquivo() + " encontrado no logsistema.transito com hash %s", hash));
                return fileWrapper;
            }

            fileWrapper.setStatusDatabaseSilent(FileStatusDatabase.DESCONHECIDO);

            logger.error("Arquivo não encontrado no logsistema.transito");
            return fileWrapper;
        } catch (SQLException ex) {
            logger.error("Erro ao buscar hash do arquivo no sistema", ex);
            throw ex;
        }
    }

    public void atualizaStatusArquivo(FileWrapper fileWrapper, FileStatusDatabase fileStatusDatabase) {

    }

    //------------------------------------------------------------------------------------------------------------------

    private String getQueryConsultaArquivoFtp() {
        return "SELECT * FROM logsistema.transitoArquivos WHERE nomeArquivo = ?";
    }
}
