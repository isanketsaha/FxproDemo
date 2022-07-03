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

    /**
     * This method will calculate the OHLC for each chunk, where chunks are created by spring batch for each minute.
     * @param quote
     * @param ohlcStage
     * @return
     */
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

    /**
     * This method is overloaded method which will calculate the OHLC for each chunk of OHLC stage for Hour and Day,
     * where chunks are created by spring batch for each Minute or Hour.
     * @param quote - incoming OHLC stage data
     * @param ohlcStage - Outgoing OHLC stage data.
     * @return OhlcStage
     */
    public OhlcStage process(OhlcStage quote, OhlcStage ohlcStage) {
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

    /**
     * This method is the processor entry class for minute batch processor.
     * @param quote
     * @return OhlcStage
     */
    public synchronized OhlcStage processMinute(Quote quote) {
        log.info("Processing for Minute : {} ", quote);
        minuteOhlcStage.setOhlcPeriod(OhlcPeriod.M1);
        return process(quote, minuteOhlcStage);

    }

    /**
     * This method is the processor entry class for hour batch processor.
     * @param quote
     * @return OhlcStage
     */
    public OhlcStage processHour(OhlcStage quote) {
        log.info("Processing for Hour : {} ", quote);
        hourOhlcStage.setOhlcPeriod(OhlcPeriod.H1);
        return process(quote, hourOhlcStage);

    }

    /**
     * This method is the processor entry class for day batch processor.
     * @param quote
     * @return OhlcStage
     */
    public OhlcStage processDay(OhlcStage quote) {
        log.info("Processing for Day : {} ", quote);
        dayOhlcStage.setOhlcPeriod(OhlcPeriod.D1);
        return process(quote, dayOhlcStage);

    }

    /**
     * Get Method for minuteOhlcStage
     * @return OhlcStage
     */
    public OhlcStage getMinuteOhlcStage() {
        return minuteOhlcStage;
    }

    /**
     * Get Method for hourOhlcStage
     * @return OhlcStage
     */
    public OhlcStage getHourOhlcStage() {
        return hourOhlcStage;
    }

    /**
     * Get Method for dayOhlcStage
     * @return OhlcStage
     */
    public OhlcStage getDayOhlcStage() {
        return dayOhlcStage;
    }

    /**
     * After Chuck data processing is completed the OHLC stage will be reset for new fresh chuck processing.
     *
     * @param stageData
     */
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
    /**
     * Data Source For Hour Job
     * @return OhlcStage
     */
    public OhlcStage fetchIncomingQuotesForHour() {
        return incomingQuotesPerHour.poll();
    }

    /**
     * Data Source For Day Job
     * @return OhlcStage
     */
    public OhlcStage fetchIncomingQuotesForDay() {
        return incomingQuotesPerDay.poll();
    }


    /**
     * Queue that will Hold Minute OHLC Stage for generating Hour OHLC Stage
     * @param incomingQuotesPerHour
     */
    public void incomingQuotesPerHour(OhlcStage incomingQuotesPerHour) {
        this.incomingQuotesPerHour.offer(incomingQuotesPerHour) ;
    }

    /**
     * Queue that will hold Hourly OHLC Stage for generating Day OHLC Stage
     * @param incomingQuotesPerDay
     */
    public void incomingQuotesPerDay(OhlcStage incomingQuotesPerDay) {
        this.incomingQuotesPerDay.offer(incomingQuotesPerDay);
    }


    /**
     * Method will convert stage data to ohlc object and push it to corresponding queue and store ohlc to database.
     * @param ohlcStage
     */
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
