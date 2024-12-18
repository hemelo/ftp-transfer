package com.hemelo.connect.enums;

public enum FileStatusEnvio {
    NAO_ENVIADO("Não Enviado"),
    ERRO_ENVIO("Erro no Envio"),
    ENVIADO("Enviado");

    private final String status;

    FileStatusEnvio(String status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return status;
    }
}
