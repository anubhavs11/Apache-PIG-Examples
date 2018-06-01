REGISTER piggybank.jar;

DEFINE SequenceFileLoader org.apache.pig.piggybank.storage.SequenceFileLoader();

seq_dataset = LOAD 'sequence-file/read' USING SequenceFileLoader AS (key:long, value:chararray);

words = FOREACH seq_dataset GENERATE FLATTEN(TOKENIZE(value)) AS word;
/*
2018-05-31 12:43:40,496 [main] INFO  org.apache.pig.impl.util.SpillableMemoryManager - Selected heap (PS Old Gen) of size 699400192 to monitor. collectionUsageThreshold = 489580128, usageThreshold = 489580128
*/
grouped = GROUP words BY word;

result = FOREACH grouped GENERATE group,COUNT(words);

dump result;

/*  STORING IN SEQUENCE FILE */
REGISTER Anubhav/elephant-bird-core-4.1.jar ;
REGISTER Anubhav/elephant-bird-pig-4.1.jar;
REGISTER Anubhav/elephant-bird-hadoop-compat-4.1.jar;

STORE result INTO 'output/pig/fileformats/sequence-file/pig-sequence' USING com.twitter.elephantbird.pig.store.SequenceFileStorage (
   '-c com.twitter.elephantbird.pig.util.TextConverter', 
   '-c com.twitter.elephantbird.pig.util.TextConverter'
 );

/*
TO READ CONTENT OF THE FILE IN TEXT FORMAT
hadoop fs -text part-r-00000;
*/

/* Avro File */
grunt> stocks = LOAD 'stocks.txt' USING PigStorage(',')  as (exchange:chararray, symbol:chararray, date:chararray, open:float, high:float, low:float, close:float, volume:int, adj_close:float);


grunt> STORE stocks INTO 'output/pig/avro' USING org.apache.pig.piggybank.storage.avro.AvroStorage(
         '{
             "schema": {
  "type": "record",
  "name": "Stock",
  "fields": [
 {"name": "exch", "type": "string"},
  {"name": "symbol",  "type": ["string", "null"]},
  {"name": "ymd", "type": ["string", "null"]},
  {"name": "price_open", "type": "float"},
  {"name": "price_high",  "type": ["float", "null"]},
  {"name": "price_low", "type": ["float", "null"]},
  {"name": "price_close", "type": "float"},
  {"name": "volume",  "type": ["int", "null"]},
  {"name": "price_adj_close", "type": ["float", "null"]}
  ]
             }
         }');

### LOAD AN AVRO FILE ###

grunt> stocks_avro = LOAD 'output/pig/avro' USING org.apache.pig.piggybank.storage.avro.AvroStorage(
		  'no_schema_check',
          	'schema_file', 
		  'avro/stocks.avro.schema');
		  
grunt> top10 = LIMIT stocks_avro 10;
grunt> DUMP top10;		  
