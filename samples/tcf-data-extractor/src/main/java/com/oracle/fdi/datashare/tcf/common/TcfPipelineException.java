package com.oracle.fdi.datashare.tcf.common;

/**
 * Unchecked exception for errors occurring during TCF pipeline
 * initialization or execution.
 */
public class TcfPipelineException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public TcfPipelineException() {
        super();
    }

    public TcfPipelineException(String message) {
        super(message);
    }

    public TcfPipelineException(String message, Throwable cause) {
        super(message, cause);
    }

    public TcfPipelineException(Throwable cause) {
        super(cause);
    }

    protected TcfPipelineException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
