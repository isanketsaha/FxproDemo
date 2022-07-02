package com.finance.ohlc.config;

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
public class MinuteSizeConfig extends SimpleCompletionPolicy implements ItemReader<Quote> {

    @Autowired
    @Qualifier(value = "itemMinuteReader")
    private SingleItemPeekableItemReader<Quote> delegate;

    private Quote currentReadItem = null;

    @Override
    public Quote read() throws Exception {
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
            final Quote nextReadItem;
            try {
                nextReadItem = delegate.peek();
                if (currentReadItem != null && nextReadItem != null) {
                    log.info("Reading For Minutes Quotes : Current {} , Next {}", currentReadItem, nextReadItem);
                    if (AppUtils.isaSameMinutes(currentReadItem.getUtcTimestamp(), nextReadItem.getUtcTimestamp())) {
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
