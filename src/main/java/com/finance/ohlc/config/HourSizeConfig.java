package com.finance.ohlc.config;

import com.finance.ohlc.domain.Ohlc;
import com.finance.ohlc.domain.OhlcStage;
import com.finance.ohlc.utils.AppUtils;
import com.finance.ohlc.vm.Quote;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.support.SingleItemPeekableItemReader;
import org.springframework.batch.repeat.RepeatContext;
import org.springframework.batch.repeat.policy.SimpleCompletionPolicy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;


@Slf4j
public class HourSizeConfig extends SimpleCompletionPolicy implements ItemReader<OhlcStage> {

    @Autowired
    @Qualifier(value = "itemHourReader")
    private SingleItemPeekableItemReader<OhlcStage> delegate;

    private OhlcStage currentReadItem = null;

    @Override
    public OhlcStage read() throws Exception {
        currentReadItem = delegate.read();
        return currentReadItem;
    }

    @Override
    public RepeatContext start(final RepeatContext context) {
        return new ComparisonPolicyTerminationContext(context);
    }

    protected class ComparisonPolicyTerminationContext extends SimpleTerminationContext {

        public ComparisonPolicyTerminationContext(final RepeatContext context) {
            super(context);
        }

        @Override
        public void update() {
            super.update();
            try {
                currentReadItem = delegate.peek();
            } catch (Exception e) {
                e.printStackTrace();
            }

        }

        @Override
        public boolean isComplete() {
            final OhlcStage nextReadItem;
            try {
                nextReadItem = delegate.peek();
                if (currentReadItem != null && nextReadItem != null) {
                    log.info("Reading For Hours Quotes : Current {} , Next {}", currentReadItem, nextReadItem);
                    if (AppUtils.isSameHour(currentReadItem.getPeriodStartUtcTimestamp(), nextReadItem.getPeriodStartUtcTimestamp())) {
                        currentReadItem.setLastItem(false);
                        return false;
                    }
                    currentReadItem.setLastItem(true);
                }
            } catch (Exception e) {
                e.printStackTrace();
                return true;
            }
            return true;
        }
    }
}
