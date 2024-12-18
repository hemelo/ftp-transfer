package com.hemelo.connect.exception;

import com.hemelo.connect.infra.Datasource;

/**
 * Excecao lancada quando ocorre um erro de conexao.
 * @see Datasource
 */
public class ConexaoException extends RuntimeException {

        public ConexaoException(String message) {
            super(message);
        }

        public ConexaoException(String message, Throwable cause) {
            super(message, cause);
        }
}
