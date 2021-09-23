package io.github.luons.engine.core.spi;

import com.google.common.base.Preconditions;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Map;

@Data
@Slf4j
public class DivideCalculator implements Calculator {

    private double scale = 1;

    private int precision = 2;

    private String numerator;

    private String denominator;

    public DivideCalculator(String numerator, String denominator) {
        this.numerator = numerator;
        this.denominator = denominator;
        Preconditions.checkNotNull(numerator);
        Preconditions.checkNotNull(denominator);
    }

    public DivideCalculator(String numerator, String denominator, double scale, int precision) {
        this(numerator, denominator);
        this.scale = scale;
        this.precision = precision;
    }

    @Override
    public double value(Map<String, Object> data) {
        double d1 = Measure.doubleValue(data, numerator);
        double d2 = Measure.doubleValue(data, denominator);
        if (d2 == 0) {
            return 0.0;
        }
        return round((d1 * scale / d2), precision);
    }

    public static double round(double x, int scale) {
        try {
            double rounded = (new BigDecimal(x)).setScale(scale, RoundingMode.HALF_UP).doubleValue();
            // return rounded == 0.0d ? 0.0d * x : rounded;
            return rounded - 0.0d == 0 ? 0.0d * x : rounded;
        } catch (NumberFormatException e) {
            log.error("round({},{}) is exception!" + e, x, scale);
            return Double.isInfinite(x) ? x : Double.NaN;
        }
    }
}
