package com.finance.ohlc.utils;

import com.finance.ohlc.domain.Ohlc;
import com.finance.ohlc.domain.OhlcStage;
import com.finance.ohlc.enumeration.OhlcPeriod;
import org.joda.time.DateTime;

/**
 * Static utils class
 */
public class AppUtils {

    private AppUtils() {

    }

    /**
     * Determine if both timestamp are in same Minutes
     * @param current
     * @param next
     * @return boolean - true if they are , otherwise false.
     */
    public static boolean isaSameMinutes(long current, long next) {
        return new DateTime(current).minuteOfHour().get() == new DateTime(next).minuteOfHour().get();
    }

    /**
     * Determine if both timestamp are in same Hour
     * @param current
     * @param next
     * @return boolean - true if they are , otherwise false.
     */
    public static boolean isSameHour(long current, long next) {
        return new DateTime(current).hourOfDay().get() == new DateTime(next).hourOfDay().get();
    }

    /**
     * Determine if both timestamp are in same Day
     * @param current
     * @param next
     * @return boolean - true if they are , otherwise false.
     */
    public static boolean isSameDay(long current, long next) {
        return new DateTime(current).dayOfMonth().get() == new DateTime(next).dayOfMonth().get();
    }

    /**
     * This method will build the OHLC object from its stage object.
     * @param stageData
     * @return Ohlc
     */
    public static Ohlc constructOhlc(OhlcStage stageData) {
        return Ohlc.builder().highPrice(stageData.getHighestPrice()).lowPrice(stageData.getLowestPrice())
                .closePrice(stageData.getClosePrice()).openPrice(stageData.getOpenPrice()).period(OhlcPeriod.M1)
                .periodStartUtcTimestamp(stageData.getPeriodStartUtcTimestamp()).build();
    }
}
