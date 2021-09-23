package io.github.luons.engine.core;

import lombok.Data;

import java.io.Serializable;

@Data
public class ResponseWrapper<T> implements Serializable {

    private int code;
    private T data;
    private String message;

    public ResponseWrapper() {
    }

    public ResponseWrapper(String message, int code) {
        this.message = message;
        this.code = code;
    }

    public ResponseWrapper(String message, int code, T data) {
        this.message = message;
        this.code = code;
        this.data = data;
    }

}
