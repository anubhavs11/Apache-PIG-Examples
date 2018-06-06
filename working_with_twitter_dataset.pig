/*
input files:

    http://vgc.poly.edu/~juliana/courses/BigData2014/Assignments/4-Pig/data/tweets.csv
    http://vgc.poly.edu/~juliana/courses/BigData2014/Assignments/4-Pig/data/users.csv 

*/



data = LOAD 'tweets.csv' USING PigStorage(',') as (id:chararray,val:chararray,name:chararray);
result1b = FILTER data BY val MATCHES '.*favorite.*';
result1b = FILTER data BY val MATCHES '.*favorite.*';
dump result1b
/*
(396124461256417280,"Honest Dm ? ❗️ favorite this",__flyygirll)
(396124464506617856,"Sleeping Naked w/ my favorite cover &gt;",JessThee_Best)
(396124504281579520,"I can't find my favorite leggings OR my Ugg boots so I'm currently having a white girl heart attack",Gracie_bee_)
*/
userdata = LOAD 'users.csv' USING PigStorage(',') AS  (name:chararray,fullname:chararray,city:chararray);
joins = JOIN data by name,userdata by name;
dump joins;

result1c = FOREACH joins GENERATE userdata::fullname,data::val;
dump result1c;
/*
...
(Bailey Baker,"I'm going to try not to be annoying about Christmas -__- but I can't make any promises 😏 ! Night 💤")
(Samuel Wilson,"@lauren_sy HBD Lol")
(John Evans,"You just wanna run over my feelings like you've been drinkin and drivin 🎧")
(Kayden Price,"I love you sooo much. @Harry_Styles")
(Kayden Price,"Please love me. @Harry_Styles")
(Faith Baker,"Must be nice to be able to go out for Halloween #adultlife")
(Ariana Rogers,"“@bradl3y2010: Does anyone else eat peanut butter with their fingers ?” You're one sick bastard")
(Samuel Gonzalez,"I swear somebody's knocking at my door at 11:59PM #thepurge")
...
*/


rightjoins = JOIN data by name RIGHT,userdata by name;

filter_data = FOREACH rightjoins GENERATE data::name as name1,userdata::name as name2,data::val as tweet;

filter_user = FILTER filter_data by name1 is NULL;
-- User which have no tweets..
dump filter_user;



grouper = GROUP data by name;

describe grouper;

--grouper: {group: chararray,data: {(id: chararray,val: chararray,name: chararray)}}

 result1d = FOREACH grouper GENERATE group , COUNT(data.name) as no_of_tweets;


dump result1d;
/*
...
(jorge_aenriquez,1)
(kelseymariee004,1)
(laurenemilydean,2)
(lolololollollll,2)
(mydaughterworld,1)
(prfessorbigsock,1)
...
*/

result1e = ORDER result1d BY no_of_tweets DESC;
dump result1e;
/*
...
( Stationnement: 10",3)
(Latinaaa_13,3)
(Blow1hoe,2)
(trevorcook42,2)
(so_nt_average,2)
( Stationnement: 20",2)
(tushi_20,2)
(erin_colombe,2)
...
*/

result1f = FILTER result1d BY no_of_tweets>=2;
dump result1f;

joins = JOIN data by name RIGHT,userdata by name;
filters = FILTER joins By data::val is NULL;
result1h = FOREACH filters GENERATE userdata::name;
dump result1h;



