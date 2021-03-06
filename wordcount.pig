data = LOAD 'read.txt' AS (line:chararray);

words = FOREACH data GENERATE FLATTEN(TOKENIZE(line)) AS word;

grouped = GROUP words BY word;

result = FOREACH grouped GENERATE group,COUNT(words);

dump result;
