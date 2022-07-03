package com.finance.ohlc.service;

import com.finance.ohlc.dao.interfaces.OhlcDao;
import com.finance.ohlc.domain.Ohlc;
import com.finance.ohlc.domain.OhlcStage;
import com.finance.ohlc.enumeration.OhlcPeriod;
import com.finance.ohlc.utils.AppUtils;
import com.finance.ohlc.vm.Quote;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

@Slf4j
@Service
@AllArgsConstructor
@NoArgsConstructor
public class CalculationService {

    private OhlcDao ohlcDao;
    private OhlcStage minuteOhlcStage;
    private OhlcStage hourOhlcStage;
    private OhlcStage dayOhlcStage;

    private Queue<OhlcStage> incomingQuotesPerHour = new ConcurrentLinkedQueue<>();
    private Queue<OhlcStage> incomingQuotesPerDay = new ConcurrentLinkedQueue<>();

    @PostConstruct
    private void init() {
        minuteOhlcStage = new OhlcStage();
        hourOhlcStage = new OhlcStage();
        dayOhlcStage = new OhlcStage();
    }

    public synchronized OhlcStage process(Quote quote, OhlcStage ohlcStage) {
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
            reset(ohlcStage);
            return ohlcStage;
        }

        return null;
    }

    public synchronized OhlcStage process(OhlcStage quote, OhlcStage ohlcStage) {
        ohlcStage.setClosePrice(quote.getClosePrice());
        if (ohlcStage.getOpenPrice() == 0) {
            ohlcStage.setOpenPrice(quote.getOpenPrice());
            ohlcStage.setPeriodStartUtcTimestamp(quote.getPeriodStartUtcTimestamp());
        }
        if (ohlcStage.getHighestPrice() < quote.getHighestPrice()) {
            ohlcStage.setHighestPrice(quote.getHighestPrice());
        }
        if (ohlcStage.getLowestPrice() > quote.getLowestPrice() || ohlcStage.getLowestPrice() == 0) {
            ohlcStage.setLowestPrice(quote.getLowestPrice());
        }
        if (quote.isLastItem()) {
            quote.setLastItem(false);
            reset(ohlcStage);
            return ohlcStage;
        }

        return null;
    }


    public synchronized OhlcStage processMinute(Quote quote) {
        log.info("Processing for Minute : {} ", quote);
        minuteOhlcStage.setOhlcPeriod(OhlcPeriod.M1);
        return process(quote, minuteOhlcStage);

    }

    public synchronized OhlcStage processHour(OhlcStage quote) {
        log.info("Processing for Hour : {} ", quote);
        hourOhlcStage.setOhlcPeriod(OhlcPeriod.H1);
        return process(quote, hourOhlcStage);

    }

    public synchronized OhlcStage processDay(OhlcStage quote) {
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

    public OhlcStage fetchIncomingQuotesForHour() {
        return incomingQuotesPerHour.poll();
    }

    public OhlcStage fetchIncomingQuotesForDay() {
        return incomingQuotesPerDay.poll();
    }


    public void incomingQuotesPerHour(OhlcStage incomingQuotesPerHour) {
        this.incomingQuotesPerHour.offer(incomingQuotesPerHour) ;
    }

    public void incomingQuotesPerDay(OhlcStage incomingQuotesPerDay) {
        this.incomingQuotesPerDay.offer(incomingQuotesPerDay);
    }


    @Transactional
    public void logOhlc(OhlcStage ohlcStage) {
        log.info("OhlcStage Data : {} ", ohlcStage);
        if (ohlcStage.getOhlcPeriod().equals(OhlcPeriod.M1)) {
            incomingQuotesPerHour(ohlcStage);
        } else if (ohlcStage.getOhlcPeriod().equals(OhlcPeriod.H1)) {
            incomingQuotesPerDay(ohlcStage);
        }
        Ohlc ohlc = AppUtils.constructOhlc(ohlcStage);
        ohlcDao.store(ohlc);
    }

}
