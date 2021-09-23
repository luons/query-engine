package io.github.luons.engine.core.utils;

import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkNotNull;

public class DateUtils {

    public static final String SEPARATOR_REGEX = "(\\s*)";
    public static final String SEPARATOR_T_REGEX = "(T?)";
    public static final String DATE_BASE_REGEX = "\\d{4}-\\d{1,2}-\\d{1,2}";

    public static final String TIME_REGEX = "^\\s*" + "\\d{1,2}:\\d{1,2}:\\d{1,2}\\s*$";
    public static final String TIME_HM_REGEX = "^\\s*" + "\\d{1,2}:\\d{1,2}\\s*$";
    public static final String DATE_MON_REGEX = "^\\s*" + "\\d{4}-\\d{1,2}" + "\\s*$";
    public static final String DATE_REGEX = "^\\s*" + DATE_BASE_REGEX + "\\s*$";
    public static final String DATE_H_REGEX = "^\\s*" + DATE_BASE_REGEX + SEPARATOR_REGEX + "\\d\\d\\s*$";
    public static final String DATE_HM_REGEX = "^\\s*" + DATE_BASE_REGEX + SEPARATOR_REGEX + "\\d{1,2}:\\dd{1,2}\\s*$";
    public static final String DATE_HMS_REGEX = "^\\s*" + DATE_BASE_REGEX + SEPARATOR_REGEX + "\\d{1,2}:\\d{1,2}:\\d{1,2}\\s*$";
    public static final String DATE_HMS_M_REGEX = "^\\s*" + DATE_BASE_REGEX + SEPARATOR_REGEX + "\\d{1,2}:\\d{1,2}:\\d{1,2}\\.\\d{1,3}\\s*$";
    public static final String DATE_T_HMS_REGEX = "^\\s*" + DATE_BASE_REGEX + SEPARATOR_T_REGEX + "\\d{1,2}:\\d{1,2}:\\d{1,2}\\s*$";
    public static final String DATE_T_HMS_M_REGEX = "^\\s*" + DATE_BASE_REGEX + SEPARATOR_T_REGEX + "\\d{1,2}:\\d{1,2}:\\d{1,2}\\.\\d{1,3}\\s*$";

    @Getter
    public enum DATE_FORMAT_ENUM {
        /**
         * 时间格式
         */
        TIME(Pattern.compile(TIME_REGEX), "HH:mm:ss"),
        TIME_HM(Pattern.compile(TIME_HM_REGEX), "HH:mm"),
        DATE_MON(Pattern.compile(DATE_MON_REGEX), "yyyy-MM"),
        DATE(Pattern.compile(DATE_REGEX), "yyyy-MM-dd"),
        DATE_H(Pattern.compile(DATE_H_REGEX), "yyyy-MM-dd HH"),
        DATE_HM(Pattern.compile(DATE_HM_REGEX), "yyyy-MM-dd HH:mm"),
        DATE_HMS(Pattern.compile(DATE_HMS_REGEX), "yyyy-MM-dd HH:mm:ss"),
        DATE_HMS_M(Pattern.compile(DATE_HMS_M_REGEX), "yyyy-MM-dd HH:mm:ss.SSS"),
        DATE_T_HMS(Pattern.compile(DATE_T_HMS_REGEX), "yyyy-MM-dd'T'HH:mm:ss"),
        DATE_T_HMS_M(Pattern.compile(DATE_T_HMS_M_REGEX), "yyyy-MM-dd'T'HH:mm:ss.SSS");

        private final Pattern pattern;

        private final String format;

        DATE_FORMAT_ENUM(Pattern pattern, String format) {
            this.pattern = pattern;
            this.format = format;
        }

        static Date formatDate(String timeString) throws Exception {
            timeString = prepare(timeString);
            for (DATE_FORMAT_ENUM formatEnum : values()) {
                Matcher matcher = formatEnum.pattern.matcher(timeString);
                if (matcher.find()) {
                    SimpleDateFormat sdf = new SimpleDateFormat(formatEnum.format);
                    return sdf.parse(timeString);
                }
            }
            throw new Exception("can not parse date: " + timeString);
        }

        private static String prepare(String timeString) {
            StringBuilder s;
            checkNotNull(timeString, ("timeString is null"));
            if (timeString.contains("-")) {
                s = new StringBuilder(timeString);
            } else if (timeString.contains("/")) {
                s = new StringBuilder(timeString.replaceAll(("[/]"), ("-")));
            } else if (timeString.contains(":") && timeString.trim().length() == 8) {
                s = new StringBuilder(getNowDateYYYYMMDD()).append(" ").append(timeString);
            } else if (timeString.contains(":") && timeString.trim().length() == 5) {
                s = new StringBuilder(getNowDateYYYYMMDD()).append(" ").append(timeString).append(":00");
            } else {
                s = new StringBuilder(timeString.substring(0, 4) + "-" + timeString.substring(4, 6) + "-" + timeString.substring(6));
            }
            int dotStart = s.indexOf(".") + 1;
            boolean hasDot = dotStart > 0;
            int msLength = dotStart + 3;
            int overMsLength = s.length() - msLength;
            int lessMsLength = msLength - s.length();

            if (hasDot && overMsLength >= 0) {
                return s.substring(0, s.length() - overMsLength);
            } else if (hasDot && lessMsLength >= 0) {
                for (int i = 0; i < lessMsLength; i++) {
                    s.append("0");
                }
                return s.toString();
            }
            if (hasDot) {
                s.append(".000");
            }
            return s.toString();
        }
    }

    public static Date currentDate() {
        return new Date();
    }

    public static long getNowMilliSecondTime() {
        return currentDate().getTime();
    }

    public static String getNowDateYYYYMMDDHHMMSS() {
        SimpleDateFormat formatter = new SimpleDateFormat(DATE_FORMAT_ENUM.DATE_HMS.format);
        return formatter.format(currentDate());
    }

    public static String getNowDateYYYYMMDD() {
        SimpleDateFormat formatter = new SimpleDateFormat(DATE_FORMAT_ENUM.DATE.format);
        return formatter.format(currentDate());
    }

    public static Date getDateBeforeDays(int amount) {
        return getDateBeforeDays(currentDate(), amount);
    }

    public static Date getDateBeforeDays(Date date, int amount) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.DAY_OF_MONTH, -amount);
        return calendar.getTime();
    }

    public static Date getLastDayOfMonth(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.set(Calendar.DAY_OF_MONTH, calendar.getActualMaximum(Calendar.DAY_OF_MONTH));
        return calendar.getTime();
    }

    public static String getNowDateFormatCustom(String format) {
        SimpleDateFormat formatter = new SimpleDateFormat(format);

        return formatter.format(currentDate());
    }

    public static long getNowDaySecondTime0() {
        Calendar c = Calendar.getInstance();
        Date d = currentDate();
        c.setTime(d);
        c.set(c.get(Calendar.YEAR), c.get(Calendar.MONTH), c.get(Calendar.DAY_OF_MONTH), 0, 0, 0);

        return c.getTime().getTime() / 1000;
    }

    public static String getNowMonth() {
        return toYYYYmm(new Date());
    }

    public static String getLastMonth() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(currentDate());
        calendar.set(Calendar.MONTH, calendar.get(Calendar.MONTH) - 1);
        return toYYYYmm(calendar.getTime());
    }

    public static String toYmdHms(long currentTime) {
        Date date = new Date(currentTime);
        SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT_ENUM.DATE_HMS.format);
        String timeStr = sdf.format(date);
        return timeStr;
    }

    public static String toYYYYmm(Date date) {
        SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT_ENUM.DATE_MON.format);
        return sdf.format(date);
    }

    public static String toYmdHms(Date date) {
        SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT_ENUM.DATE_HMS.format);
        return sdf.format(date);
    }

    public static Date toDate(String timeString) throws Exception {
        if (StringUtils.isEmpty(timeString)) {
            return new Date();
        }
        return DATE_FORMAT_ENUM.formatDate(timeString);
    }

    public static Long toLong(Date date) {
        if (null == date) {
            return null;
        }
        return date.getTime() * 1000;
    }

    public static Long toLong(String timeString) throws Exception {
        if (null == timeString) {
            return null;
        }
        return toLong(toDate(timeString));
    }

    public static String dateFormat(Date date, String dateFormat) {
        if (date == null) {
            return "";
        }
        SimpleDateFormat format = new SimpleDateFormat(dateFormat);
        try {
            return format.format(date);
        } catch (Exception ex) {

        }
        return "";
    }

    public static Date add(Date date, int amount) {
        return add(date, Calendar.DAY_OF_MONTH, amount);
    }

    public static Date add(Date date, int field, int amount) {

        if (date == null) {
            date = new Date();
        }

        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(field, amount);

        return cal.getTime();
    }

}
