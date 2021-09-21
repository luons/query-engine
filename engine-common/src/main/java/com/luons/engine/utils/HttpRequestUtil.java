package com.luons.engine.utils;

public final class HttpRequestUtil {

    public static boolean isInformational(int httpStatus) {
        return httpStatus >= 100 && httpStatus < 200;
    }

    public static boolean isSuccessful(int httpStatus) {
        return httpStatus >= 200 && httpStatus < 300;
    }

    public static boolean isRedirection(int httpStatus) {
        return httpStatus >= 300 && httpStatus < 400;
    }

    public static boolean isClientError(int httpStatus) {
        return httpStatus >= 400 && httpStatus < 500;
    }

    public static boolean isServerError(int httpStatus) {
        return httpStatus >= 500 && httpStatus < 600;
    }
}
