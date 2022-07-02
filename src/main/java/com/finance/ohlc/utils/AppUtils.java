package com.finance.ohlc.utils;

import com.finance.ohlc.domain.Ohlc;
import com.finance.ohlc.domain.OhlcStage;
import com.finance.ohlc.enumeration.OhlcPeriod;
import org.joda.time.DateTime;


public class AppUtils {

    private AppUtils() {

    }

    public static boolean isaSameMinutes(long current, long next) {
        return new DateTime(current).minuteOfHour().get() == new DateTime(next).minuteOfHour().get();
    }

    public static boolean isSameHour(long current, long next) {
        return new DateTime(current).hourOfDay().get() == new DateTime(next).hourOfDay().get();
    }

    public static boolean isSameDay(long current, long next) {
        return new DateTime(current).dayOfMonth().get() == new DateTime(next).dayOfMonth().get();
    }

    public static synchronized Ohlc constructOhlc(OhlcStage stageData) {
        return Ohlc.builder().highPrice(stageData.getHighestPrice()).lowPrice(stageData.getLowestPrice())
                .closePrice(stageData.getClosePrice()).openPrice(stageData.getOpenPrice()).period(OhlcPeriod.M1)
                .periodStartUtcTimestamp(stageData.getPeriodStartUtcTimestamp()).build();
    }
}
