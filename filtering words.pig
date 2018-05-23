
data = LOAD 'read.txt' AS (line:chararray);

words = FOREACH data GENERATE FLATTEN(TOKENIZE(line)) as word;

word_size = FOREACH words GENERATE word,SIZE(word) as width;

result = FILTER word_size BY width==1;

DUMP result;
