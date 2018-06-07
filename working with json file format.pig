REGISTER /root/Downloads/Anubhav/jar_files/json_jars/elephant-bird-hadoop-compat-4.1.jar;
REGISTER /root/Downloads/Anubhav/jar_files/json_jars/elephant-bird-pig-4.1.jar;
REGISTER /root/Downloads/Anubhav/jar_files/json_jars/json-simple-1.1.jar;
cd assignment4

--1. WORKING WITH SIMPLE JSON DATA (without junk spaces in input format)
data1 = LOAD 'artist.json' USING JsonLoader('id:chararray,last_name:chararray,first_name:chararray,year_of_birth:chararray');


--2. WORKING WITH SIMPLE JSON DATA (with junk spaces in input format)
data = LOAD 'artist.json' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') AS jsonMap;

describe data;
--{jsonMap: bytearray}
dump data;
--([last_name#Spacey,id#artist:18,first_name#Kevin,year_of_birth#1959])
artist = FOREACH data GENERATE jsonMap#'id' AS id,
 jsonMap#'last_name' AS lastname,
 jsonMap#'first_name' AS firstname,
 jsonMap#'year_of_birth' AS YearOfBirth;

describe artist;
--artist: {id: bytearray,lastname: bytearray,firstname: bytearray,YearOfBirth: bytearray}
dump artist;
--(artist:18,Spacey,Kevin,1959)


--3. WORKING WITH NESTED JSON DATA (without junk spaces in input format)
data = LOAD 'movies.json' USING JsonLoader('id:chararray,title:chararray,year:chararray,genre:chararray,summary:chararray,country:chararray,directory:(id:chararray,lastname:chararray,firstname:chararray,yearOfBirth:chararray),actors:{(id:chararray,role:chararray)}');

describe data;
/*
data: {id: chararray,title: chararray,year: chararray,genre: chararray,summary: chararray,country: chararray,directory: (id: chararray,lastname: chararray,firstname: chararray,yearOfBirth: chararray),actors: {(id: chararray,role: chararray)}}
*/
dump data;
/*
(movie:14,Se7en,1995,Crime, Two detectives, a rookie and a veteran, hunt a serial killer who usestheseven,USA,(artist:31,Fincher,David,1962),{(artist:18,Doe),(artist:22,Somerset),(artist:32,Mills)})
*/

-- STORING DATA IN JSON FORMAT
STORE data INTO 'output' USING JsonStorage();


filter_data = FOREACH data GENERATE .. $3, $5 .. ;

dump filter_data;
--(movie:14,Se7en,1995,Crime,USA,(artist:31,Fincher,David,1962),{(artist:18,Doe),(artist:22,Somerset),(artist:32,Mills)})

filter_data = FOREACH data GENERATE title,year;

grouper = GROUP filter_data BY year;
DESCRIBE grouper;
--grouper: {group: chararray,filter_data: {(title: chararray,year: chararray)}}

mUS_year = FOREACH grouper GENERATE group,filter_data.title;
dump mUS_year;
--(1995,{(Se7en)})


grouper = GROUP data BY directory;
dump grouper;
/*
((artist:31,Fincher,David,1962),{(movie:14,Se7en,1995,Crime, Two detectives, a rookie and a veteran, hunt a serial killer who usestheseven,USA,(artist:31,Fincher,David,1962),{(artist:18,Doe),(artist:22,Somerset),(artist:32,Mills)})})
*/
describe grouper;
/*
grouper: {group: (id: chararray,lastname: chararray,firstname: chararray,yearOfBirth: chararray),data: {(id: chararray,title: chararray,year: chararray,genre: chararray,summary: chararray,country: chararray,directory: (id: chararray,lastname: chararray,firstname: chararray,yearOfBirth: chararray),actors: {(id: chararray,role: chararray)})}}
*/
mUS_director = FOREACH grouper GENERATE group,data.title;
dump mUS_director;
--((artist:31,Fincher,David,1962),{(Se7en)})


--NOT WORKING
dataset = FILTER data BY country == 'USA';

dataset = FOREACH dataset GENERATE id,actors;
DESCRIBE dataset;
--dataset: {id: chararray,actors: {(id: chararray,role: chararray)}}

mUS_actors = FOREACH data GENERATE id,FLATTEN(actors);
DESCRIBE mUS_actors;
--mUS_actors: {id: chararray,actors::id: chararray,actors::role: chararray}
dump mUS_actors;
/*
(movie:14,artist:18,Doe)
(movie:14,artist:22,Somerset)
(movie:14,artist:32,Mills)
*/


dataset = FOREACH data GENERATE title,actors;
describe dataset;
--dataset: {title: chararray,actors: {(id: chararray,role: chararray)}}

dump dataset
--(Se7en,{(artist:18,Doe),(artist:18,Mills),(artist:22,Somerset),(artist:32,Mills)})
dataset = FOREACH dataset GENERATE title,FLATTEN($1);
describe dataset;
--dataset: {title: chararray,actors::id: chararray,actors::role: chararray}
dump dataset;
/*
(Se7en,artist:18,Doe)
(Se7en,artist:18,Mills)
(Se7en,artist:22,Somerset)
(Se7en,artist:32,Mills)
*/
grouper = GROUP dataset BY (title,actors::id);
describe grouper;
--grouper: {group: (title: chararray,actors::id: chararray),dataset: {(title: chararray,actors::id: chararray,actors::role: chararray)}}
dump grouper;
/*
((Se7en,artist:18),{(Se7en,artist:18,Mills),(Se7en,artist:18,Doe)})
((Se7en,artist:22),{(Se7en,artist:22,Somerset)})
((Se7en,artist:32),{(Se7en,artist:32,Mills)})
*/
flatten_group = FOREACH grouper GENERATE group.title,group.actors::id,dataset.actors::role;
describe flatten_group;
--flatten_group: {title: chararray,actors::id: chararray,{(actors::role: chararray)}}
dump flatten_group;
/*
(Se7en,artist:18,{(Mills),(Doe)})
(Se7en,artist:22,{(Somerset)})
(Se7en,artist:32,{(Mills)})
*/

junk = FOREACH flatten_group GENERATE title,actors::id as id ,BagToString($2) as roles;

describe junk;
--junk: {title: chararray,id: chararray,roles: chararray}
dump junk;
/*
(Se7en,artist:18,Mills_Doe)
(Se7en,artist:22,Somerset)
(Se7en,artist:32,Mills)
*/

vals = FOREACH junk GENERATE title,id, STRSPLIT(roles,'_') as role;
describe vals;
--vals: {title: chararray,id: chararray,role: ()}

dump vals;
/*
(Se7en,artist:18,(Mills,Doe))
(Se7en,artist:22,(Somerset))
(Se7en,artist:32,(Mills))
*/





