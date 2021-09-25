package io.github.luons.engine.spi;

public interface ResultCode {

    /**
     * 错误码编号
     *
     * @return Integer
     */
    Integer getCode();

    /**
     * 错误码描述
     *
     * @return String
     */
    String getMessage();

    /**
     * 必须提供toString的实现
     *
     * @return String
     */
    @Override
    String toString();

    /**
     * 统一字符格式化
     *
     * @param message 字符信息
     * @return String
     */
    String format(String message);
}
