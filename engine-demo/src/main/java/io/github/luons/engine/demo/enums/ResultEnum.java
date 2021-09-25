package io.github.luons.engine.demo.enums;

import io.github.luons.engine.spi.ResultCode;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.http.HttpStatus;

@Getter
@AllArgsConstructor
public enum ResultEnum implements ResultCode {

    /**
     *
     */
    PARAM_ERROR(1, "参数不正确"),
    OK(HttpStatus.SC_OK, "成功"),
    INTERNAL_SERVER_ERROR(HttpStatus.SC_INTERNAL_SERVER_ERROR, "服务异常"),
    UNAUTHORIZED(HttpStatus.SC_UNAUTHORIZED, "没有权限"),
    BLANK(120, "数据为空！"),
    NONE_EXIST(121, "不存在！"),
    CREATE_OR_MODIFY_ERROR(122, "创建或修改信息失败！"),
    ALREADY_EXIST(123, "已存在！"),
    FILTER_OR_SEARCH_ERROR(124, "查询或搜索失败！"),
    CREATE_ERROR(125, "创建失败！"),
    MODIFY_ERROR(126, "更新失败！"),
    DELETE_ERROR(127, "删除失败！"),
    ;
    /**
     * 状态码
     */
    private final Integer code;
    /**
     * 信息
     */
    private final String message;

    @Override
    public String format(String message) {
        return message + this.message;
    }

}
