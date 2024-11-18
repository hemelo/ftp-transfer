package com.hemelo.connect.enums;

public enum TopicEmail {
    TESTE("teste", "[Info][Connect] Teste de envio de email"),
    ALERTA_GRAVACAO_LOG_ARQUIVOS("erroGravacaoLogsFtpDownload", "[Alerta][Connect] Erro ao gravar log de arquivos enviados e perdidos do FTP"),
    ARQUIVO_ENVIADO_PARA_CONNECT("envioArquivosConnect", "[Info][Connect] Arquivo(s) disponibilizados para envio via connect."),
    ALERTA_DOWNLOAD_ARQUIVOS_FTP("downloadArquivosFtp", "[Alerta][Connect] Erro ao baixar arquivo(s) do FTP"),
    ARQUIVO_NAO_ENVIADO_ERRO_PARA_CONNECT("erroEnvioArquivosConnect", "[Alerta][Connect] Erro ao enviar arquivo(s) para o Connect"),
    RELATORIO("relatorioHoraConnect", "[Info][Connect] Relatório de execução do Connect");

    private final String subject;
    private final String subscriptionTopic;

    TopicEmail(String subscriptionTopic, String subject) {
        this.subscriptionTopic = subscriptionTopic;
        this.subject = subject;
    }

    @Override
    public String toString() {
        return subject;
    }

    public String getSubscriptionTopic() {
        return subscriptionTopic;
    }
}
