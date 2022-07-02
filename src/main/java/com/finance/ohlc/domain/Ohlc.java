package com.finance.ohlc.domain;

import com.finance.ohlc.enumeration.OhlcPeriod;
import lombok.Builder;
import lombok.Data;
import lombok.ToString;

@Data
@ToString
@Builder
public class Ohlc {
    double openPrice;
    double highPrice;
    double lowPrice;
    double closePrice;
    OhlcPeriod period;
    long periodStartUtcTimestamp;
}
