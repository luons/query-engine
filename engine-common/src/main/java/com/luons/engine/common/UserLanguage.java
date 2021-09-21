package com.luons.engine.common;

public class UserLanguage {

    public static final String LANGUE_CHINESE = "zh";

    public static final String LANGUE_ENGLISH = "en";

    public static final String LANGUE_DEFAULT = LANGUE_CHINESE;

    private static ThreadLocal<String> language = new ThreadLocal<>();

    private UserLanguage() {
    }

    public static void setLanguage(String lang) {
        language.set(lang);
    }

    public static void clearLanguage() {
        language.set(null);
    }

    public static String getLanguage() {
        String lang = language.get();
        return lang == null ? LANGUE_DEFAULT : lang;
    }
}
