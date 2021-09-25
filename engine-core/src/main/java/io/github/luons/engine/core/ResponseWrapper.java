package io.github.luons.engine.core;

import io.github.luons.engine.common.MessageWrapper;
import lombok.Data;

import java.io.Serializable;

@Data
public class ResponseWrapper<T> extends MessageWrapper implements Serializable {

    private int code = 200;

    private String message = "success";

    private T data;

    private long timestamp = System.currentTimeMillis();

    public ResponseWrapper() {
    }

    public ResponseWrapper(T data) {
        this.data = data;
    }

    public ResponseWrapper(int code, String message) {
        this.code = code;
        this.message = message;
    }

    public ResponseWrapper(int code, String message, T data) {
        this.code = code;
        this.message = message;
        this.data = data;
    }

    public static <T> ResponseWrapper<T> success() {
        return new ResponseWrapper<>();
    }

    public static <T> ResponseWrapper<T> success(T data) {
        return new ResponseWrapper<>(data);
    }

    public static <T> ResponseWrapper<T> success(Integer code, String message, T data) {
        return new ResponseWrapper<>(code, message, data);
    }

    public static <T> ResponseWrapper<T> error(String message) {
        return new ResponseWrapper<>((500), message);
    }

    public static <T> ResponseWrapper<T> error(Integer code, String message) {
        return new ResponseWrapper<>(code, message);
    }

    public static <T> ResponseWrapper<T> error(Integer code, String message, T data) {
        return new ResponseWrapper<>(code, message, data);
    }
}
