package com.finance.ohlc.vm;

public interface Quote {

    boolean isLastItem();

    void setLastItem(boolean lastItem);

    int getId();

    double getPrice();

    long getInstrumentId();

    long getUtcTimestamp();
}
