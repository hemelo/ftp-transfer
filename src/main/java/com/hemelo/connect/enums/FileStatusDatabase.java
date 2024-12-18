package com.hemelo.connect.enums;

public enum FileStatusDatabase {
    DESCONHECIDO("Desconhecido"),
    ENVIADO("Enviado");

    private final String status;

    FileStatusDatabase(String status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return status;
    }
}
