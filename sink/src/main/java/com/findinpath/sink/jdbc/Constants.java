package com.findinpath.sink.jdbc;

import java.util.Calendar;
import java.util.TimeZone;

class Constants {
    public static final Calendar TZ_UTC = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

    private Constants(){
    }
}
