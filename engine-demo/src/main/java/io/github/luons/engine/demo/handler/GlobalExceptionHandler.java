package io.github.luons.engine.demo.handler;

import io.github.luons.engine.common.CommonException;
import io.github.luons.engine.core.ResponseWrapper;
import io.github.luons.engine.demo.enums.ResultEnum;
import lombok.extern.slf4j.Slf4j;
import org.springframework.validation.BindingResult;
import org.springframework.validation.FieldError;
import org.springframework.validation.ObjectError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    /**
     * 总异常
     *
     * @param e 异常
     * @return 错误信息
     */
    @ExceptionHandler(Exception.class)
    public ResponseWrapper<Object> exceptionHandler(Exception e) {
        return ResponseWrapper.error(ResponseWrapper.SC_INTERNAL_SERVER_ERROR, e.getMessage());
    }

    /**
     * 系统内部异常
     *
     * @param e 异常
     * @return 错误信息
     */
    @ExceptionHandler(CommonException.class)
    public ResponseWrapper<Object> stardbExceptionHandler(CommonException e) {
        return ResponseWrapper.error(e.getCode(), e.getMessage());
    }

    /**
     * 数据库操作异常
     *
     * @param e 异常
     * @return 错误信息
     */
    @ExceptionHandler(SQLException.class)
    public ResponseWrapper<Object> sqlExceptionExceptionHandler(SQLException e) {
        return ResponseWrapper.error(ResultEnum.CREATE_OR_MODIFY_ERROR.getCode(), e.getMessage());
    }

    /**
     * REST请求参数校验
     *
     * @param e 异常
     * @return 错误信息
     */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseWrapper<Object> validationBodyException(MethodArgumentNotValidException e) {
        List<String> errors = new ArrayList<>();
        BindingResult result = e.getBindingResult();
        if (result.hasErrors()) {
            List<ObjectError> allErrors = result.getAllErrors();
            allErrors.forEach(objectError -> {
                FieldError err = (FieldError) objectError;
                errors.add(err.getDefaultMessage());
                log.error("【param is not valid】: name={}, field={}, message={}", err.getObjectName(), err.getField(),
                        err.getDefaultMessage());
            });
        }
        return ResponseWrapper.error(ResultEnum.PARAM_ERROR.getCode(),
                ResultEnum.PARAM_ERROR.getMessage(), String.join((";"), errors));
    }
}
