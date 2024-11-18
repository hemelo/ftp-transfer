package com.hemelo.connect.enums;

public enum FileStatusRemoto {

    EM_TRANSFERENCIA("Em transferência"),
    NECESSARIO_VERIFICACAO("Necessário verificação"),
    TRANSFERIDO("Transferido"),
    REFERENCIA_PERDIDA("Referência perdida"),
    DELETADO("Deletado");

    private String status;

    FileStatusRemoto(String status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return status;
    }
}
