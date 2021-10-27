package io.github.luons.engine.core.enums;

public enum Operator {
    /**
     * Operator
     */
    DEFAULT(" ="),
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

    Operator(String expInSql) {
        this.expInSql = expInSql;
    }

    public String getExpInSql() {
        return expInSql;
    }
}
