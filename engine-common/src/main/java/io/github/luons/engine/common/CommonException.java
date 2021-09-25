package io.github.luons.engine.common;

import io.github.luons.engine.spi.ResultCode;
import lombok.Data;

import java.io.PrintWriter;
import java.io.StringWriter;

@Data
public class CommonException extends RuntimeException {

    private Integer code;

    public CommonException(String message) {
        super(message);
        this.code = MessageWrapper.SC_INTERNAL_SERVER_ERROR;
    }

    public CommonException(Integer code, String message) {
        super(message);
        this.code = code;
    }

    public CommonException(ResultCode resultEnum) {
        super(resultEnum.getMessage());
        this.code = resultEnum.getCode();
    }

    public CommonException(ResultCode dataCode, String message) {
        super(dataCode.format(message));
        this.code = dataCode.getCode();
    }

    private CommonException(ResultCode dataCode, String message, Throwable cause) {
        super(dataCode.format(message) + " _ " + getMessage(cause), cause);
        this.code = dataCode.getCode();
    }

    public static CommonException asException(ResultCode dataCode) {
        return new CommonException(dataCode);
    }

    public static CommonException asException(ResultCode dataCode, String message) {
        return new CommonException(dataCode, message);
    }

    public static CommonException asException(ResultCode dataCode, Throwable cause) {
        return asException(dataCode, dataCode.getMessage(), cause);
    }

    public static CommonException asException(ResultCode dataCode, String message, Throwable cause) {
        if (cause instanceof CommonException) {
            return (CommonException) cause;
        }
        return new CommonException(dataCode, message, cause);
    }

    private static String getMessage(Object obj) {
        if (obj == null) {
            return "";
        }
        if (obj instanceof Throwable) {
            StringWriter str = new StringWriter();
            PrintWriter pw = new PrintWriter(str);
            ((Throwable) obj).printStackTrace(pw);
            return str.toString();
        } else {
            return obj.toString();
        }
    }
}
