### LOAD stocks ###

grunt> stocks = LOAD 'stocks.txt' USING PigStorage(',') as (exchange:chararray, symbol:chararray, date:chararray, open:float, high:float, low:float, close:float,volume:int, adj_close:float);

### LOAD dividends ###

grunt> divs = LOAD 'div.txt' USING PigStorage(',') as (exchange:chararray, symbol:chararray, date:chararray, dividends:float);


### JOIN ###

join_symbol = JOIN divs BY (symbol,date), stocks BY (symbol,date);
