# FxPro Calculation Service




The solution is based on Spring Batch.  
We have Built three jobs (Minute, Hour, Day), the data first processed through Minute job to evaluate OHLC for each Minute, then each Minute OHLC is passed to Hour Job through a Queue then the day job process the Hour OHLC.
Each batch has its reader, processor, and writer.  
Here the reader creates a chunk based on the timeframe(Minute, Hour, Day).  
Once we create bundles we pass them through the processor which calls CalculationService and calculates the OHLC for the bundle of data and at the end it resets the stage object.
The writer writes the OHLCStage object to logOhlc() of CalculationService, and this method has been made transactional and has logic to convert the stage object to OHLC.

Each component has been made async to accommodate huge throughput.

Install the dependency and run the maven project.



    mvn clean install

    