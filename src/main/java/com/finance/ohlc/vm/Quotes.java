package com.finance.ohlc.vm;

import lombok.Builder;
import lombok.Data;
import lombok.ToString;

@Data
@Builder
@ToString
public class Quotes implements Quote {

    int id;
    boolean lastItem;
    double price;
    long instrumentId;
    long utcTimestamp;

}
