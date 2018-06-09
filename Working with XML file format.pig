
REGISTER piggybank.jar;

data = LOAD 'xmlfile.xml' USING org.apache.pig.piggybank.storage.XMLLoader('BOOK') AS (x:chararray);

DESCRIBE data;
--data: {x: chararray}

DEFINE XPath org.apache.pig.piggybank.evaluation.xml.XPath();

--WORKING WITH SIMPLE DATA IN XML
/* SAMPLE INPUT */
/*
<CATALOG>
<BOOK>
<TITLE>Hadoop Defnitive Guide</TITLE>
<AUTHOR>Tom White</AUTHOR>
<COUNTRY>US</COUNTRY>
<COMPANY>CLOUDERA</COMPANY>
<PRICE>24.90</PRICE>
<YEAR>2012</YEAR>
</BOOK>
</CATALOG>
*/
B = FOREACH data GENERATE XPath(x,'BOOK/TITLE') AS Title,XPath(x,'BOOK/AUTHOR') AS author,XPath(x,'BOOK/COUNTRY') as country,XPath(x,'BOOK/COMPANY') as company,XPath(x,'BOOK/PRICE') as price,XPath(x,'BOOK/YEAR') as year;

DESCRIBE B;
--B: {Title: chararray,author: chararray,country: chararray,company: chararray,price: chararray,year: chararray}
DUMP B;
/*
(Hadoop Defnitive Guide,Tom White,US,CLOUDERA,24.90,2012)
(Programming Pig,Alan Gates,USA,Horton Works,30.90,2013)
*/
-- WORKING WITH NESTED DATA IN XML
/* SAMPLE INPUT */
/*
<CATALOG>
<BOOK>
<TITLE>Hadoop Defnitive Guide</TITLE>
<AUTHOR><FIRST>Tom</FIRST>
	<LAST>White</LAST></AUTHOR>
<COUNTRY>US</COUNTRY>
<COMPANY>CLOUDERA</COMPANY>
<PRICE>24.90</PRICE>
<YEAR>2012</YEAR>
</BOOK>
</CATALOG>
*/
B = FOREACH data GENERATE XPath(x,'BOOK/TITLE') AS Title,XPath(x,'BOOK/AUTHOR/FIRST') AS Firstname,XPath(x,'BOOK/AUTHOR/LAST') AS Lastname,XPath(x,'BOOK/COUNTRY') as country,XPath(x,'BOOK/COMPANY') as company,XPath(x,'BOOK/PRICE') as price,XPath(x,'BOOK/YEAR') as year;
DESCRIBE B;
--B: {Title: chararray,Firstname: chararray,Lastname: chararray,country: chararray,company: chararray,price: chararray,year: chararray}
DUMP B;
/*
(Hadoop Defnitive Guide,Tom,White,US,CLOUDERA,24.90,2012)
(Programming Pig,Alan,Gates,USA,Horton Works,30.90,2013)
*/

STORE B INTO 'xml_to_csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage();
/*
root@kali:~/Downloads/assignment4/xml_to_csv# cat part-m-00000
Hadoop Defnitive Guide,Tom,White,US,CLOUDERA,24.90,2012
Programming Pig,Alan,Gates,USA,Horton Works,30.90,2013
/*
