package com.hemelo.connect.constants;

import org.apache.commons.lang3.time.DateFormatUtils;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class Dates {

    public static final String HOUR_FORMAT = "HH:mm:ss";
    public static final DateTimeFormatter HOUR_FORMATTER = DateTimeFormatter.ofPattern(HOUR_FORMAT);

    public static final String BRAZILIAN_DATE_FORMAT = "dd/MM/yyyy";
    public static final String BRAZILIAN_DATE_TIME_FORMAT = "dd/MM/yyyy HH:mm:ss";
    public static final String BRAZILIAN_DATE_FORMAT_FS = "ddMMyyyy";
    public static final String BRAZILIAN_DATE_TIME_FORMAT_FS = "ddMMyyyyHHmmss";
    public static final String UNIVERSAL_DATETIME_FORMAT = "yyyyMMddHHmmssSSS";

    public static final DateTimeFormatter UNIVERSAL_DATETIME_FORMATTER = DateTimeFormatter.ofPattern(UNIVERSAL_DATETIME_FORMAT);
    public static final DateTimeFormatter BRAZILIAN_DATE_TIME_FORMATTER_FS = DateTimeFormatter.ofPattern(BRAZILIAN_DATE_TIME_FORMAT_FS);
    public static final DateTimeFormatter BRAZILIAN_DATE_FORMATTER = DateTimeFormatter.ofPattern(BRAZILIAN_DATE_FORMAT);
    public static final DateTimeFormatter BRAZILIAN_DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern(BRAZILIAN_DATE_TIME_FORMAT);
    public static final DateTimeFormatter BRAZILIAN_DATE_FORMATTER_FS = DateTimeFormatter.ofPattern(BRAZILIAN_DATE_FORMAT_FS);

    public static final ZoneId ZONE_ID = ZoneId.of("America/Sao_Paulo");
}

