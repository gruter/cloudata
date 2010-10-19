CREATE TABLE <table name> ( <col1>, <col2>, ... ) [version = 3]
DROP TABLE <table name>

INSERT INTO t_test VALUES ( ('aaa', 'bbb') ) WHERE ROWKEY = '1'

INSERT INTO t_test (col1) VALUES ( ('aaa', 'bbb') ) WHERE ROWKEY = '1'

INSERT INTO t_test FROM 'hdfs://host01:9000/user/test/t_test/input' [DELIM = '\t'] [ROW_READER = 'class name']

SELECT * FROM <table name> WHERE ...
SELECT <col1, col2, ...> FROM <table name> WHERE...

SELECT * FROM t_test
SELECT col1,col2 FROM t_test
SELECT col1,col2 FROM t_test WHERE ROWKEY = '1' 
SELECT col1,col2 FROM t_test WHERE ROWKEY = '1' AND col1 = '2' 
SELECT col1,col2 FROM t_test WHERE ROWKEY LIKE '1%' 
SELECT col1,col2 FROM t_test WHERE ROWKEY BETWEEN '1' AND '2' 

SELECT sum(col1), sum(col2.key) FROM t_test WHERE ROWKEY = '1' 

SELECT a.col1, 

DELETE FROM t_test WHERE ROWKEY = '1' [AND col1 = '2']
DELETE FROM t_test WHERE ROWKEY = '1' [AND col1 = '2']


SELECT BATCH col1, col2 
  FROM t_test
 WHERE sum(col1.key) > 10
 
SELECT BATCH col1, col2 
  FROM t_test
  INTO t_output (o_col1, o_col2) values (col1, col2) ROWKEY = t_test.ROWKEY 
 WHERE sum(col1) > 10
 
SELECT BATCH col1, col2 
  FROM t_test
  INTO 'hdfs://host01:9000/user/test' values (col1, col2) ROWKEY = t_test.ROWKEY 

SELECT BATCH col1, col2 
  FROM t_test
  INTO 'local://home/test.dat'

SELECT BATCH a.col1, b.col2
  FROM t_test1 as a, 
       t_test2 as b
  INTO t_ouput

SELECT a.col1, (SELECT b.col2 FROM t_test2 as b WHERE b.rowkey = a.col2)
  FROM t_test2 as a    
  
  
  
Web page

CREATE TABLE t_web (url, title, page) comment = 'web page'
 
INSERT INTO t_web (url, title, page) 
  FROM read('hdfs://host01:9001/user/hadoop/crawl', 'org.cloudata.WebPageReader')

CREATE TABLE t_term (docid)

SELECT BATCH parse(url, page, 'org.cloudata.TermRowParser')
  FROM t_web
  INTO t_term (docid) values (ROW_PARSER.1) ROWKEY = ROW_PARSER.0 
  

SELECT b.url, b.title  
  FROM t_term as a, t_web as b
 WHERE top(a.docid, 5) = b.ROWKEY
   AND a.ROWKEY = 'java'  
   
SELECT url, title
  FROM t_web
 WHERE ROWKEY IN ( SELECT top(docid, 5) FROM t_term WHERE ROWKEY = 'java')   
   