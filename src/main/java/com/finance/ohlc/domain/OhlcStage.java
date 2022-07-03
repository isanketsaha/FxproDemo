package com.finance.ohlc.domain;


import com.finance.ohlc.enumeration.OhlcPeriod;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * This is an intermediate class of OHLC .
 */
@Data
@ToString
@NoArgsConstructor
public class OhlcStage {

    private volatile OhlcPeriod ohlcPeriod;
    private volatile double highestPrice;
    private volatile double lowestPrice;
    private volatile double closePrice;
    private volatile long periodStartUtcTimestamp;
    private volatile double openPrice;
    private volatile boolean lastItem;
}
