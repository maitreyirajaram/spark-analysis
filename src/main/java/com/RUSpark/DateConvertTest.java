package com.RUSpark;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import static java.lang.Integer.parseInt;


public class DateConvertTest {

    public static void main(String[] args) {
        String unix_timestamp = "1618105402";
        long unix_sec = Long.parseLong(unix_timestamp);
        Date date = new Date((unix_sec)*1000L);
        SimpleDateFormat jdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
        jdf.setTimeZone(TimeZone.getTimeZone("EST"));
        String date_full = jdf.format(date);
        String key = date_full.split(" ")[1].split(":")[0];
        System.out.println(key);


        Double sampleDecimal = 3.3333333;
        System.out.println(sampleDecimal);
    }
}
