package com.luons.engine.core.enums;


public enum Keyword {

    /**
     * SQL 关键字
     */
    WHERE("WHERE"),

    ASC("ASC"),
    DESC("DESC"),

    ;

    private String name;

    private Keyword(String name) {
        this.name = name;
    }

    public String getExpInSql() {
        return name;
    }
}
