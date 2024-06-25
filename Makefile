make all: 
	make create_dirs
	make upload_input
	hadoop com.sun.tools.javac.Main *.java
	jar cf Hw3.jar *.class


start:
	sudo rm -R /tmp/*
	hdfs namenode -format
	start-all.sh
	jps

stop:
	stop-all.sh

run-all:
	make total
	make jobtitle
	make titleexperince
	make employeeresidence
	make averageyear

total:
	hadoop jar Hw3.jar Hw3 total /input/salaries.csv output_total


jobtitle:
	hadoop jar Hw3.jar Hw3 jobtitle /input/salaries.csv output_jobtitle

titleexperince:
	hadoop jar Hw3.jar Hw3 titleexperince /input/salaries.csv output_titleexperince

employeeresidence:
	hadoop jar Hw3.jar Hw3 employeeresidence /input/salaries.csv output_employeeresidence
	

averageyear:
	hadoop jar Hw3.jar Hw3 averageyear /input/salaries.csv output_averageyear
	
upload_input:
	hadoop fs -put salaries.csv /input/salaries.csv

create_dirs:
	hadoop fs -mkdir /input
	hadoop fs -mkdir /output

clean: 
	hadoop fs -rm -r /input
	hadoop fs -rm -r /output
	hadoop fs -rm -r /user
	hadoop fs -rm -r /tmp
	rm -f *.class
	rm -f Hw3.jar
