# hive_udf
UDF, GenericUDF, UDTF, UDAF
#Create a table
Create a table named employee:
```sql
CREATE TABLE employee(
	name			STRING,
	salary			FLOAT,
	subordinates	ARRAY<STRING>,
	deductions		MAP<STRING, FLOAT>,
	address			STRUCT<street:STRING, city:STRING, state:STRING, zip:INT>
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
COLLECTION ITEMS TERMINATED BY '\002'
MAP KEYS TERMINATED BY '\003'
LINES TERMINATED BY '\n'
```
Load the data that is in data directory.
```shell
hadoop fs -put employee.txt /hive
```
```sql
LOAD DATA INPATH '/hive/employees.txt'
OVERWRITE INTO TABLE employee
```
#Compile and package
Compile and package the source codes by maven.First of all, enter the project's root directory, then execute the follow commands in shell:
```shell
$ mvn clean
$ mvn clean compile
$ mvn clean package
```
#Run the examples
If we successfully installed Hadoop and Hive before, we are able to run the examples now. Firstly, we should add the jar into CLASSPATH:
```shell
hive> ADD jar /root/experiment/hive/hive-0.0.1-SNAPSHOT.jar;
```
Secondly, create the temporary functions:
```shell
hive> CREATE TEMPORARY FUNCTION hello 
    > AS 'edu.wzm.hive.HelloUDF';
hive> CREATE TEMPORARY FUNCTION contains 
    > AS 'edu.wzm.hive.ComplexUDFExample';
hive> CREATE TEMPORARY FUNCTION collect 
    > AS 'edu.wzm.hive.udaf.GenericUDAFCollect';
hive> CREATE TEMPORARY FUNCTION explode_name 
    > AS 'edu.wzm.hive.udtf.ExplodeNameUDTF';
````
finally, test the example:
```shell
hive> SELECT hell(name)
    > FROM employee;
hive> SELECT contains(subordinates, subordinates[0]), subordinates
    > FROM employee;
hive> SELECT collect(name)
    > FROM employee;
hive> SELECT salary, concat_ws(',', collect(name))
    > FROM employee;    
hive> SELECT explode_name(name)
    > FROM employee;
``` 
