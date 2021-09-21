package com.luons.engine.core.exception;

import static org.apache.http.HttpStatus.*;

public class EngineException extends RuntimeException {

    private final int statusCode;

    public EngineException(String message) {
        this(message, null);
    }

    public EngineException(String message, Throwable e) {
        this(0, message, e);
    }

    public EngineException(int statusCode, String message) {
        this(statusCode, message, null);
    }

    public EngineException(int statusCode, String message, Throwable e) {
        super(message, e);
        this.statusCode = statusCode;
    }

    public final int getStatusCode() {
        return statusCode;
    }

    public boolean isHttpError() {
        return statusCode > 0;
    }

    public boolean isServerError() {
        return statusCode >= 500 && statusCode < 600;
    }

    public boolean isClientError() {
        return statusCode >= 400 && statusCode < 500;
    }

    public boolean isInternalServerError() {
        return statusCode == SC_INTERNAL_SERVER_ERROR;
    }

    public boolean isServiceUnavailable() {
        return statusCode == SC_SERVICE_UNAVAILABLE;
    }

    public boolean isBadRequest() {
        return statusCode == SC_BAD_REQUEST;
    }

    public boolean isNotFound() {
        return statusCode == SC_NOT_FOUND;
    }

    public boolean isUnauthorized() {
        return statusCode == SC_UNAUTHORIZED;
    }

    public boolean isForbidden() {
        return statusCode == SC_FORBIDDEN;
    }

    public boolean isConflict() {
        return statusCode == SC_CONFLICT;
    }

    public boolean isPreconditionFailed() {
        return statusCode == SC_PRECONDITION_FAILED;
    }

}
