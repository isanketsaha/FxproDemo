package com.finance.ohlc.service.interfaces;

import com.finance.ohlc.domain.Ohlc;
import com.finance.ohlc.enumeration.OhlcPeriod;
import com.finance.ohlc.listener.QuoteListener;

import java.util.List;

public interface OhlcService extends QuoteListener {

    /**
     * latest non persisted OHLC
     */
    Ohlc getCurrent(long instrumentId, OhlcPeriod period);

    /**
     * all OHLCs which are kept in a database
     */
    List<Ohlc> getHistorical(long instrumentId, OhlcPeriod period);

    /**
     * latest non persisted OHLC and OHLCs which are kept in a database
     */
    List<Ohlc> getHistoricalAndCurrent(long instrumentId, OhlcPeriod period);
}
