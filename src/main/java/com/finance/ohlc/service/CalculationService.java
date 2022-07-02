package com.finance.ohlc.service;

import com.finance.ohlc.dao.interfaces.OhlcDao;
import com.finance.ohlc.domain.Ohlc;
import com.finance.ohlc.domain.OhlcStage;
import com.finance.ohlc.enumeration.OhlcPeriod;
import com.finance.ohlc.utils.AppUtils;
import com.finance.ohlc.vm.Quote;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;

@Slf4j
@Service
public class CalculationService {

    @Autowired
    private OhlcDao ohlcDao;

    private OhlcStage minuteOhlcStage;
    private OhlcStage hourOhlcStage;
    private OhlcStage dayOhlcStage;

    @PostConstruct
    private void init() {
        minuteOhlcStage = new OhlcStage();
        hourOhlcStage = new OhlcStage();
        dayOhlcStage = new OhlcStage();
    }

    public synchronized Ohlc process(Quote quote, OhlcStage ohlcStage) {
        ohlcStage.setClosePrice(quote.getPrice());
        if (ohlcStage.getOpenPrice() == 0) {
            ohlcStage.setOpenPrice(quote.getPrice());
            ohlcStage.setPeriodStartUtcTimestamp(quote.getUtcTimestamp());
        }
        if (ohlcStage.getHighestPrice() < quote.getPrice()) {
            ohlcStage.setHighestPrice(quote.getPrice());
        }
        if (ohlcStage.getLowestPrice() > quote.getPrice() || ohlcStage.getLowestPrice() == 0) {
            ohlcStage.setLowestPrice(quote.getPrice());
        }
        if (quote.isLastItem()) {
            quote.setLastItem(false);
            Ohlc ohlc = AppUtils.constructOhlc(ohlcStage);
            reset(ohlcStage);
            return ohlc;
        }

        return null;
    }


    public synchronized Ohlc processMinute(Quote quote) {
        log.info("Processing for Minute : {} ", quote);
        minuteOhlcStage.setOhlcPeriod(OhlcPeriod.M1);
        return process(quote, minuteOhlcStage);

    }

    public synchronized Ohlc processHour(Quote quote) {
        log.info("Processing for Hour : {} ", quote);
        hourOhlcStage.setOhlcPeriod(OhlcPeriod.H1);
        return process(quote, hourOhlcStage);

    }

    public synchronized Ohlc processDay(Quote quote) {
        log.info("Processing for Day : {} ", quote);
        dayOhlcStage.setOhlcPeriod(OhlcPeriod.D1);
        return process(quote, dayOhlcStage);

    }

    public OhlcStage getMinuteOhlcStage() {
        return minuteOhlcStage;
    }

    public OhlcStage getHourOhlcStage() {
        return hourOhlcStage;
    }

    public OhlcStage getDayOhlcStage() {
        return dayOhlcStage;
    }

    private void reset(OhlcStage stageData) {
        if (stageData.getOhlcPeriod().equals(OhlcPeriod.M1)) {
            minuteOhlcStage = new OhlcStage();
        }
        if (stageData.getOhlcPeriod().equals(OhlcPeriod.H1)) {
            hourOhlcStage = new OhlcStage();
        }
        if (stageData.getOhlcPeriod().equals(OhlcPeriod.H1)) {
            dayOhlcStage = new OhlcStage();
        }
    }

    @Transactional
    public void logOhlc(Ohlc ohlc) {
        log.info("OHLC Data : {} ", ohlc);
        ohlcDao.store(ohlc);
    }

}
