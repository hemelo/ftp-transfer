package com.hemelo.connect.enums;

public enum FileStatusLocal {
    DISPONIVEL("Disponível"),
    DISPONIVEL_INVALIDO("Disponível com erro"),
    INDISPONIVEL("Indisponível");

    private final String status;

    FileStatusLocal(String status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return status;
    }
}
