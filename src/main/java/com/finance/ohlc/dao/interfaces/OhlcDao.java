package com.finance.ohlc.dao.interfaces;

import com.finance.ohlc.domain.Ohlc;
import com.finance.ohlc.enumeration.OhlcPeriod;

import java.util.List;

public interface OhlcDao {

    void store(Ohlc ohlc);

    /**
     * loads OHLCs from DB selected by parameters and sorted by
     * periodStartUtcTimestamp in descending order
     */
    List<Ohlc> getHistorical(long instrumentId, OhlcPeriod period);
}
