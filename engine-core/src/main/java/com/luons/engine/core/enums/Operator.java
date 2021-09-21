package com.luons.engine.core.enums;

public enum Operator {
    /**
     * Operator
     */
    EQ(" ="),
    NE(" !="),
    LT(" <"),
    GT(" >"),
    LE(" <="),
    GE(" >="),
    IN(" IN"),
    NIN(" NOT IN"),
    LIKE(" LIKE"),
    EXIST(" EXIST");

    private String expInSql;

    private Operator(String expInSql) {
        this.expInSql = expInSql;
    }

    public String getExpInSql() {
        return expInSql;
    }
}
