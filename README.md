# FxPro Calculation Service




The solution is based on Spring Batch.  
We have Built three jobs (Minute, Hour, Day) that fetch data from our OhlcServiceImpl.
Each batch has its reader, processor, and writer.  
Here the reader creates a chunk based on the timeframe(Minute, Hour, Day).  
Once we create bundles we pass them through the processor which calls CalculationService and calculate the OHLC for the bundle of data and at the end it reset the stage object.
The writer writes the OHLC object to logOhlc() of CalculationService, and this method has been made transactional.

Each component has been made async to accommodate huge throughput.

Install the dependency and run the maven project.



    mvn clean install

    