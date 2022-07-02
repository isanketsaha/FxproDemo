package com.finance.ohlc.listener;

import com.finance.ohlc.vm.Quote;

public interface QuoteListener {

    void onQuote(Quote quote);
}
