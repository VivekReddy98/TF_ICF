javac TFICF.java
jar cf TFICF.jar TFICF*.class
spark-submit --class TFICF TFICF.jar input &> spark_output.txt
grep -v '^2020\|^(\|^-' spark_output.txt > output.txt
grep -v '^2020' spark_output.txt > comp_output.txt
