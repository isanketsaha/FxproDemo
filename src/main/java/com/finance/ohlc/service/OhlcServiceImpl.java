package com.finance.ohlc.service;

import com.finance.ohlc.dao.interfaces.OhlcDao;
import com.finance.ohlc.domain.Ohlc;
import com.finance.ohlc.domain.OhlcStage;
import com.finance.ohlc.enumeration.OhlcPeriod;
import com.finance.ohlc.service.interfaces.OhlcService;
import com.finance.ohlc.utils.AppUtils;
import com.finance.ohlc.vm.Quote;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

@Service
@AllArgsConstructor
@NoArgsConstructor
public class OhlcServiceImpl implements OhlcService {


    @Autowired
    private CalculationService calculationService;

    @Autowired
    private OhlcDao ohlcDao;

    private Queue<Quote> incomingQuotesPerMinute = new ConcurrentLinkedQueue<>();


    @Override
    public void onQuote(Quote quote) {
        incomingQuotesPerMinute.offer(quote);
    }

    /**
     * Incoming quotes will be fetched from queue to Minute Job
     * @return
     */
    public Quote fetchIncomingQuotesForMinute() {
        return incomingQuotesPerMinute.poll();
    }


    @Override
    public Ohlc getCurrent(long instrumentId, OhlcPeriod period) {
        if (period.equals(OhlcPeriod.M1)) {
            return AppUtils.constructOhlc(calculationService.getMinuteOhlcStage());
        } else if (period.equals(OhlcPeriod.H1)) {
            return AppUtils.constructOhlc(calculationService.getHourOhlcStage());
        } else {
            return AppUtils.constructOhlc(calculationService.getDayOhlcStage());
        }
    }

    @Override
    public List<Ohlc> getHistorical(long instrumentId, OhlcPeriod period) {
        return ohlcDao.getHistorical(instrumentId, period);
    }

    @Override
    public List<Ohlc> getHistoricalAndCurrent(long instrumentId, OhlcPeriod period) {
        List<Ohlc> historical = getHistorical(instrumentId, period);
        Ohlc current = getCurrent(instrumentId, period);
        historical.add(current);
        return historical;
    }
}
