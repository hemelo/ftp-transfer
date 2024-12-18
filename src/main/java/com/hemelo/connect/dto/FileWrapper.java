package com.hemelo.connect.dto;

import com.hemelo.connect.constants.Dates;
import com.hemelo.connect.enums.FileStatusDatabase;
import com.hemelo.connect.enums.FileStatusEnvio;
import com.hemelo.connect.enums.FileStatusLocal;
import com.hemelo.connect.enums.FileStatusRemoto;
import com.hemelo.connect.exception.HashAlteradoException;
import com.hemelo.connect.utils.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.net.ftp.FTPFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;

/**
 * Classe que encapsula um arquivo, com informações sobre o arquivo e seu status
 * É um objeto complexo que contém informações sobre o arquivo no FTP, na máquina local e sobre o envio do arquivo para o Connect
 */
public class FileWrapper {

    private static final Logger logger = LoggerFactory.getLogger(FileWrapper.class);

    private String parent;

    private Instant ftpLatestUpdate, dbLatestUpdate, downloadLatestUpdate, envioLatestUpdate, enviadoAt, downloadedAt, findedAt;

    private String ftpCaminhoBase;
    private String localCaminhoBase;
    private String envioCaminhoBase;

    private FTPFile ftpFile;

    private FileStatusDatabase statusDatabase;
    private FileStatusRemoto statusFtp;
    private FileStatusLocal statusLocal;
    private FileStatusEnvio statusEnvio;

    private Boolean isCredenciado = false;

    private String hash, hashDatabase;

    private String adicional;

    public FileWrapper(FTPFile ftpFile) {
        this.findedAt = Instant.now();
        this.ftpLatestUpdate = Instant.now();
        this.ftpFile = ftpFile;
        this.statusFtp = FileStatusRemoto.NECESSARIO_VERIFICACAO;
        this.statusLocal = FileStatusLocal.INDISPONIVEL;
        this.statusEnvio = FileStatusEnvio.NAO_ENVIADO;
    }

    @Override
    public boolean equals(Object object) {
        if (object instanceof FTPFile _ftpFile) {
            return this.ftpFile.getName().equals(_ftpFile.getName());
        }

        if (object instanceof FileWrapper _FileWrapper) {
            return this.ftpFile.getName().equals(_FileWrapper.getFtpFile().getName()) && this.ftpCaminhoBase.equals(_FileWrapper.getFtpCaminhoBase());
        }

        return false;
    }

    // Getters e Setters

    public FTPFile getFtpFile() {
        return ftpFile;
    }

    public void setFtpFile(FTPFile ftpFile) {
        this.ftpFile = ftpFile;
        this.ftpLatestUpdate = Instant.now();
    }

    public FileStatusRemoto getStatusFtp() {
        return statusFtp;
    }

    public void setStatusFtp(FileStatusRemoto statusFtp) {

        if (this.statusFtp != statusFtp)
            logger.debug("Status FTP do arquivo " + getNomeArquivo() + " alterado de \"" + this.statusFtp + "\" para \"" + statusFtp + "\"");

        this.statusFtp = statusFtp;

        if (statusFtp != FileStatusRemoto.REFERENCIA_PERDIDA && statusFtp != FileStatusRemoto.DELETADO) {

            if (this.ftpLatestUpdate == null) {
                logger.debug("Atualizando data de última verificação do arquivo " + getNomeArquivo() + " no FTP para " + LocalDateTime.ofInstant(Instant.now(), Dates.ZONE_ID).format(Dates.BRAZILIAN_DATE_TIME_FORMATTER));

            } else {
                logger.debug("Atualizando data de última verificação do arquivo " + getNomeArquivo() + " no FTP de " + LocalDateTime.ofInstant(this.ftpLatestUpdate, Dates.ZONE_ID).format(Dates.BRAZILIAN_DATE_TIME_FORMATTER) + " para " + LocalDateTime.ofInstant(Instant.now(), Dates.ZONE_ID).format(Dates.BRAZILIAN_DATE_TIME_FORMATTER));
            }

            this.ftpLatestUpdate = Instant.now();
        }

    }

    public String getFtpCaminhoBase() {
        return ftpCaminhoBase;
    }

    public void setFtpCaminhoBase(String ftpCaminhoBase) {
        this.ftpCaminhoBase = ftpCaminhoBase;
    }

    public Instant getFtpLatestUpdate() {
        return ftpLatestUpdate;
    }

    public Instant getFindedAt() {
        return findedAt;
    }

    public boolean isCredenciado() {
        return isCredenciado;
    }

    public void setIsCredenciado(Boolean isCredenciado) {
        isCredenciado = isCredenciado;
    }

    public FileStatusLocal getStatusLocal() {
        return statusLocal;
    }

    public void setStatusLocal(FileStatusLocal statusLocal) {

        if (this.statusLocal != statusLocal)
            logger.debug("Status local do arquivo " + getNomeArquivo() + " alterado de \"" + this.statusLocal + "\"  para \"" + statusLocal + "\"");

        this.statusLocal = statusLocal;

        if (this.downloadLatestUpdate == null) {
            logger.debug("Atualizando data de última verificação do arquivo " + getNomeArquivo() + " na máquina local para " + LocalDateTime.ofInstant(Instant.now(), Dates.ZONE_ID).format(Dates.BRAZILIAN_DATE_TIME_FORMATTER));

        } else {
            logger.debug("Atualizando data de última atualização do arquivo " + getNomeArquivo() + " na máquina local de " + LocalDateTime.ofInstant(this.downloadLatestUpdate, Dates.ZONE_ID).format(Dates.BRAZILIAN_DATE_TIME_FORMATTER) + " para " + LocalDateTime.ofInstant(Instant.now(), Dates.ZONE_ID).format(Dates.BRAZILIAN_DATE_TIME_FORMATTER));
        }

        this.downloadLatestUpdate = Instant.now();

        if (statusLocal == FileStatusLocal.DISPONIVEL) {

            if (this.downloadedAt == null) {
                logger.debug("Atualizando data de download do arquivo " + getNomeArquivo() + " para " + LocalDateTime.ofInstant(Instant.now(), Dates.ZONE_ID).format(Dates.BRAZILIAN_DATE_TIME_FORMATTER));

            } else {
                logger.debug("Atualizando data de download do arquivo " + getNomeArquivo() + " de " + LocalDateTime.ofInstant(this.downloadedAt, Dates.ZONE_ID).format(Dates.BRAZILIAN_DATE_TIME_FORMATTER) + " para " + LocalDateTime.ofInstant(Instant.now(), Dates.ZONE_ID).format(Dates.BRAZILIAN_DATE_TIME_FORMATTER));
            }

            this.downloadedAt = Instant.now();
        }
    }

    public void setStatusLocalSilent(FileStatusLocal statusLocal) {

        this.statusLocal = statusLocal;
        this.downloadLatestUpdate = Instant.now();

        if (statusLocal == FileStatusLocal.DISPONIVEL) {
            this.downloadedAt = Instant.now();
        }
    }

    public String getLocalCaminhoBase() {
        return localCaminhoBase;
    }

    public void setLocalCaminhoBase(String localCaminhoBase) {
        this.localCaminhoBase = localCaminhoBase;
    }

    public String getParent() {
        return parent;
    }

    public void setParent(String parent) {
        this.parent = parent;
    }

    public FileStatusEnvio getStatusEnvio() {
        return statusEnvio;
    }

    public void setStatusEnvio(FileStatusEnvio statusEnvio) {

        if (this.statusEnvio != statusEnvio)
            logger.debug("Status de envio do arquivo " + getNomeArquivo() + " alterado de \"" + this.statusEnvio + "\" para \"" + statusEnvio + "\"");

        this.statusEnvio = statusEnvio;

        if (this.envioLatestUpdate == null) {
            logger.debug("Atualizando data de última atualização do envio do arquivo " + getNomeArquivo() + " para o connect para " + LocalDateTime.ofInstant(Instant.now(), Dates.ZONE_ID).format(Dates.BRAZILIAN_DATE_TIME_FORMATTER));

        } else {
            logger.debug("Atualizando data de última atualização do envio do arquivo " + getNomeArquivo() + " para o connect de " + LocalDateTime.ofInstant(this.envioLatestUpdate, Dates.ZONE_ID).format(Dates.BRAZILIAN_DATE_TIME_FORMATTER) + " para " + LocalDateTime.ofInstant(Instant.now(), Dates.ZONE_ID).format(Dates.BRAZILIAN_DATE_TIME_FORMATTER));
        }

        this.envioLatestUpdate = Instant.now();

        if (statusEnvio == FileStatusEnvio.ENVIADO) {

            if (this.enviadoAt == null) {
                logger.debug("Atualizando data de envio do arquivo " + getNomeArquivo() + " para o connect para " + LocalDateTime.ofInstant(Instant.now(), Dates.ZONE_ID).format(Dates.BRAZILIAN_DATE_TIME_FORMATTER));

            } else {
                logger.debug("Atualizando data de envio do arquivo " + getNomeArquivo() + " para o connect de " + LocalDateTime.ofInstant(this.enviadoAt, Dates.ZONE_ID).format(Dates.BRAZILIAN_DATE_TIME_FORMATTER) + " para " + LocalDateTime.ofInstant(Instant.now(), Dates.ZONE_ID).format(Dates.BRAZILIAN_DATE_TIME_FORMATTER));
            }

            this.enviadoAt = Instant.now();
        }
    }

    public void setStatusEnvioSilent(FileStatusEnvio statusEnvio) {
        this.statusEnvio = statusEnvio;
        this.envioLatestUpdate = Instant.now();

        if (statusEnvio == FileStatusEnvio.ENVIADO) {
            this.enviadoAt = Instant.now();
        }
    }

    public String getNomeArquivo() {
        return ftpFile.getName();
    }

    public String getFtpCaminhoCompleto() {
        return ftpCaminhoBase +  "/" + getNomeArquivo();
    }

    public String getLocalCaminhoCompleto() {
        return localCaminhoBase + File.separator + getNomeArquivo();
    }

    public String getCaminhoEnvioCompleto() {
        return envioCaminhoBase + File.separator + getNomeArquivo();
    }

    public Instant getEnviadoAt() {
        return enviadoAt;
    }

    public Instant getDownloadedAt() {
        return downloadedAt;
    }

    public Instant getDownloadLatestUpdate() {
        return downloadLatestUpdate;
    }

    public Instant getEnvioLatestUpdate() {
        return envioLatestUpdate;
    }

    public void setEnvioCaminhoBase(String envioCaminhoBase) {
        this.envioCaminhoBase = envioCaminhoBase;
    }

    /**
     * Calcula o hash do arquivo e verifica se ele foi alterado
     * @return Hash do arquivo
     * @throws IOException Se o arquivo não for encontrado
     * @throws HashAlteradoException Se o hash do arquivo foi alterado
     */
    public String getHash() throws IOException, HashAlteradoException {

        if (StringUtils.isBlank(getLocalCaminhoCompleto()) || !new File(getLocalCaminhoCompleto()).exists()) {

            if (StringUtils.isNotBlank(hash)) {
                return hash;
            }

            throw new FileNotFoundException("Arquivo não encontrado: " + getLocalCaminhoCompleto());
        }

        String newHash = FileUtils.calculateFileHash1(getLocalCaminhoCompleto());

        if (StringUtils.isBlank(hash)) {
            logger.debug("Calculando hash do arquivo " + getNomeArquivo() + " pela primeira vez");
        } else {

            newHash = newHash.toUpperCase();

            if (!newHash.equalsIgnoreCase(hash)) {
                logger.warn("Hash do arquivo " + getNomeArquivo() + " alterado de " + hash + " para " + newHash);

                throw new HashAlteradoException("Hash do arquivo " + getNomeArquivo() + " alterado de " + hash + " para " + newHash);
            } else {
                logger.debug("Atualizando hash do arquivo " + getNomeArquivo() + " de " + hash + " para " + newHash);
            }
        }

        hash = newHash;
        return hash;
    }
    
    public String getHashDatabase() {
        return hashDatabase;
    }
    
    public void setHashDatabase(String hashDatabase) {
        this.hashDatabase = hashDatabase;
    }

    public boolean possuiHashDatabase() {
        return !StringUtils.isBlank(this.getHashDatabase());
    }

    public boolean comparaHashDatabase() {

        if (!this.possuiHashDatabase()) {
            throw new IllegalStateException("Arquivo " + getNomeArquivo() + " não possui hash no banco de dados ou não foi realizada consulta");
        }

        String hash;

        try {
            hash = this.getHash();
        } catch (IOException | HashAlteradoException e) {
            logger.error("Erro ao calcular hash do arquivo " + getNomeArquivo(), e);
            return false;
        }

        return hash.equalsIgnoreCase(this.getHashDatabase());
    }

    public void setStatusDatabase(FileStatusDatabase statusDatabase) {
        if (this.statusDatabase != statusDatabase)
            logger.debug("Status de arquivo " + getNomeArquivo() + " no banco de dados alterado de \"" + this.statusEnvio + "\" para \"" + statusEnvio + "\"");

        this.statusDatabase = statusDatabase;

        if (this.dbLatestUpdate == null) {
            logger.debug("Atualizando data de última atualização do status do arquivo " + getNomeArquivo() + " no banco de dados para " + LocalDateTime.ofInstant(Instant.now(), Dates.ZONE_ID).format(Dates.BRAZILIAN_DATE_TIME_FORMATTER));

        } else {
            logger.debug("Atualizando data de última atualização do status do arquivo " + getNomeArquivo() + " no banco de dados de " + LocalDateTime.ofInstant(this.envioLatestUpdate, Dates.ZONE_ID).format(Dates.BRAZILIAN_DATE_TIME_FORMATTER) + " para " + LocalDateTime.ofInstant(Instant.now(), Dates.ZONE_ID).format(Dates.BRAZILIAN_DATE_TIME_FORMATTER));
        }

        this.dbLatestUpdate = Instant.now();
    }

    public void setStatusDatabaseSilent(FileStatusDatabase statusDatabase) {
        this.statusDatabase = statusDatabase;
        this.dbLatestUpdate = Instant.now();
    }

    public FileStatusDatabase getStatusDatabase() {
        return statusDatabase;
    }

    public Instant getDbLatestUpdate() {
        return dbLatestUpdate;
    }

    public String getAdicional() {
        return adicional;
    }

    public void setAdicional(String adicional) {
        this.adicional = adicional;
    }
}
