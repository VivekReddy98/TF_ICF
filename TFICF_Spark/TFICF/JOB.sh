javac TFICF.java
jar cf TFICF.jar TFICF*.class
spark-submit --class TFICF TFICF.jar input &> spark_output.txt
