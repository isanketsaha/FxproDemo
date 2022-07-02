package com.finance.ohlc.dao;

import com.finance.ohlc.dao.interfaces.OhlcDao;
import com.finance.ohlc.domain.Ohlc;
import com.finance.ohlc.enumeration.OhlcPeriod;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class OhlcDaoImpl implements OhlcDao {

    @Override
    public void store(Ohlc ohlc) {
        //Implementation by co-worker.
    }

    @Override
    public List<Ohlc> getHistorical(long instrumentId, OhlcPeriod period) {
        //Implementation by co-worker.
        return null;
    }
}
