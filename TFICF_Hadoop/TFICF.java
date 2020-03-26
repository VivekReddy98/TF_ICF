import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import java.io.IOException;
import java.util.*;
import java.lang.*;

/*
 * Main class of the TFICF MapReduce implementation.
 * Author: Tyler Stocksdale
 * Date:   10/18/2017
 */
public class TFICF {

    public static void main(String[] args) throws Exception {
        // Check for correct usage
        if (args.length != 2) {
            System.err.println("Usage: TFICF <input corpus0 dir> <input corpus1 dir>");
            System.exit(1);
        }

		// return value of run func
		int ret = 0;

		// Create configuration
		Configuration conf0 = new Configuration();
		Configuration conf1 = new Configuration();

		// Input and output paths for each job
		Path inputPath0 = new Path(args[0]);
		Path inputPath1 = new Path(args[1]);
        try{
            ret = run(conf0, inputPath0, 0);
        }catch(Exception e){
            e.printStackTrace();
        }
        if(ret == 0){
        	try{
            	run(conf1, inputPath1, 1);
        	}catch(Exception e){
            	e.printStackTrace();
        	}
        }

     	System.exit(ret);
    }

	public static int run(Configuration conf, Path path, int index) throws Exception{
		// Input and output paths for each job

		Path wcInputPath = path;
		Path wcOutputPath = new Path("output" +index + "/WordCount");
		Path dsInputPath = wcOutputPath;
		Path dsOutputPath = new Path("output" + index + "/DocSize");
		Path tficfInputPath = dsOutputPath;
		Path tficfOutputPath = new Path("output" + index + "/TFICF");

		// Get/set the number of documents (to be used in the TFICF MapReduce job)
    FileSystem fs = path.getFileSystem(conf);
    FileStatus[] stat = fs.listStatus(path);
		String numDocs = String.valueOf(stat.length);
		conf.set("numDocs", numDocs);

		// Delete output paths if they exist
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(wcOutputPath))
			hdfs.delete(wcOutputPath, true);
		if (hdfs.exists(dsOutputPath))
			hdfs.delete(dsOutputPath, true);
		if (hdfs.exists(tficfOutputPath))
			hdfs.delete(tficfOutputPath, true);

		 // Create and execute Word Count job
     Job WCjob = Job.getInstance(conf, "Word Count");

     // Set Respective Classes
     WCjob.setJarByClass(TFICF.class);
     WCjob.setMapperClass(WCMapper.class);
     WCjob.setCombinerClass(WCReducer.class); // Combiner essentially does the same thing as Reducer but inside one Node.
     WCjob.setReducerClass(WCReducer.class);

     // Since the output of this job is an Int Writable i.e. Word Count, IntWritable class is set as the OutPutValue class.
     WCjob.setOutputKeyClass(Text.class);
     WCjob.setOutputValueClass(IntWritable.class);

     // Set input and the output File paths in HDFS
     FileInputFormat.addInputPath(WCjob, wcInputPath);
     FileOutputFormat.setOutputPath(WCjob, wcOutputPath);

     // Wait for the completion of the Job
     WCjob.waitForCompletion(true);

		// Create and execute Document Size job
    Job DSjob = Job.getInstance(conf, "Document Size");

    // Set Respective Classes
    DSjob.setJarByClass(TFICF.class);
    DSjob.setMapperClass(DSMapper.class);
    DSjob.setReducerClass(DSReducer.class);

    // The output in Text, Text
    DSjob.setOutputKeyClass(Text.class);
    DSjob.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(DSjob, dsInputPath);
    FileOutputFormat.setOutputPath(DSjob, dsOutputPath);

    // Wait for the completion of the Job
    DSjob.waitForCompletion(true);


		//Create and execute TFICF job
    Job TFICFjob = Job.getInstance(conf, "TFIDF Computation");

    // Set Respective Classes
    TFICFjob.setJarByClass(TFICF.class);
    TFICFjob.setMapperClass(TFICFMapper.class);
    TFICFjob.setReducerClass(TFICFReducer.class);

    // The output in Text, Text
    TFICFjob.setOutputKeyClass(Text.class);
    TFICFjob.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(TFICFjob, tficfInputPath);
    FileOutputFormat.setOutputPath(TFICFjob, tficfOutputPath);

    // Wait for completion of the Job and return the value.
    return TFICFjob.waitForCompletion(true) ? 0 : 1;
  }

	/*
	 * Creates a (key,value) pair for every word in the document
	 *
	 * Input:  ( byte offset , contents of one line )
	 * Output: ( (word@document) , 1 )
	 *
	 * word = an individual word in the document
	 * document = the filename of the document
	 */
	public static class WCMapper extends Mapper<Object, Text, Text, IntWritable> {

	    private final static IntWritable one = new IntWritable(1);
	    private Text word = new Text();

      // Overriden method from Mapper Class
	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	      FileSplit fileSplit = (FileSplit)context.getInputSplit();

        // Get filename of the document
        String filename = fileSplit.getPath().getName();

	      String line = value.toString();

        // Convert the line to an array of strings by splitting by delimitter.
        String[] words = line.toLowerCase().split("\\s+");

        for (String tmp : words){
            // Loop over all the Words and remove Special characters such as these specified below.
            tmp = tmp.replaceAll("[,\\.\\(\\{\\}\\*\\=\\?\\!\\)\'\";:&]", "");

            // Nullify all the words which doesn't start with a character [a-z]
            tmp = tmp.replaceAll("^[^a-z]+.*", "");

            // Process only those which arent null
            if (!tmp.isEmpty() && !StringUtils.isNumeric(tmp)){
              word.set(tmp + "@" + filename);

              // Emit the Key, Value pair to the context object.
              context.write(word, one);
            }
	      }
	    }

    }

    /*
	 * For each identical key (word@document), reduces the values (1) into a sum (wordCount)
	 *
	 * Input:  ( (word@document) , 1 )
	 * Output: ( (word@document) , wordCount )
	 *
	 * wordCount = number of times word appears in document
	 */
	  public static class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	    private IntWritable result = new IntWritable();

	    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
           // Set a Variable to count wordCount.
           int wordCount = 0;

           // Loop over the Int Writable and find the wordCount for a particular word@document
      		 for (IntWritable val : values) {
      		   wordCount += val.get();
      		 }
      		 result.set(wordCount);

           // Emit the Key, Value pair to the context object.
      		 context.write(key, result);
	    }

    }

	/*
	 * Rearranges the (key,value) pairs to have only the document as the key
	 *
	 * Input:  ( (word@document) , wordCount )
	 * Output: ( document , (word=wordCount) )
	 */
	public static class DSMapper extends Mapper<Object, Text, Text, Text> {

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

      // Split the text based on Whitespace
      String[] words = value.toString().split("\\s+");

      // Then split the first value on delimitter "@"
      String[] keyWords = words[0].split("@");

      // Emit the Key, Value pair to the context object.
      context.write(new Text(keyWords[1]), new Text(keyWords[0] + "=" + words[1]));
    }

  }

    /*
	 * For each identical key (document), reduces the values (word=wordCount) into a sum (docSize)
	 *
	 * Input:  ( document , (word=wordCount) )
	 * Output: ( (word@document) , (wordCount/docSize) )
	 *
	 * docSize = total number of words in the document
	 */
	public static class DSReducer extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

          int totalWords = 0; // totalWords Variable for a particular document.
          Map<String, String> TEMPMap = new HashMap<String, String>();

          for (Text val : values){ // Loop over all the Iterable and compute totalWords .
              String[] arrOfStr = val.toString().split("=");
              TEMPMap.put(arrOfStr[0], arrOfStr[1]);
              totalWords += Integer.parseInt(arrOfStr[1]);
          }

          String docSize = Integer.toString(totalWords);

          for (String val : TEMPMap.keySet()){
              // Create a key and the value in respective format and emit the Key, Value pair to the context object.
              context.write(new Text(val+"@"+key.toString()), new Text(TEMPMap.get(val)+"/"+docSize)); // this.KEYword,this.VALUEword);
          }
    }

    }

	/*
	 * Rearranges the (key,value) pairs to have only the word as the key
	 *
	 * Input:  ( (word@document) , (wordCount/docSize) )
	 * Output: ( word , (document=wordCount/docSize) )
	 */
	public static class TFICFMapper extends Mapper<Object, Text, Text, Text> {

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      // Split the text basdd on Whitespace
      String[] words = value.toString().split("\\s+");

      // Split the text basdd on "@"
      String[] keyWords = words[0].split("@");

      // Emit the Key, Value pair to the context object in the format specified on the top.
      context.write(new Text(keyWords[0]), new Text(keyWords[1] + "=" + words[1])); //this.Keyword, this.Valueword);
    }

  }

    /*
    * For each identical key (word), reduces the values (document=wordCount/docSize) into a
    * the final TFICF value (TFICF). Along the way, calculates the total number of documents and
    * the number of documents that contain the word.
    *
    * Input:  ( word , (document=wordCount/docSize) )
    * Output: ( (document@word) , TFICF )
    *
    * numDocs = total number of documents
    * numDocsWithWord = number of documents containing word
    * TFICF = ln(wordCount/docSize + 1) * ln(numDocs/numDocsWithWord +1)
    *
    * Note: The output (key,value) pairs are sorted using TreeMap ONLY for grading purposes. For
    *       extremely large datasets, having a for loop iterate through all the (key,value) pairs
    *       is highly inefficient!
    */
	public static class TFICFReducer extends Reducer<Text, Text, Text, Text> {

		private static int numDocs;
		private Map<Text, Text> tficfMap = new HashMap<>();

		// gets the numDocs value and store it in numDocs Variable, this will run once in the very beggining
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			numDocs = Integer.parseInt(conf.get("numDocs"));
		}

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

      Map<String, String> TEMPMap = new HashMap<String, String>();
      int numDocsWithWord = 0; // Variable to store numDocsWithWord

      for (Text val: values){ // Loop over the values iterable and store the document
          String[] arrStr = val.toString().split("=");
          TEMPMap.put(arrStr[0], arrStr[1]);
          numDocsWithWord++;
      }

      for (String val : TEMPMap.keySet()){
          String[] words = TEMPMap.get(val).split("/");

          // Compute Intermediate values for Term Frequency and Inverse Corpus Frequency
          double tf_inter = Double.valueOf(Double.valueOf(words[0])/Double.valueOf(words[1]));
          double idf_inter = ((double)numDocs+1)/((double)numDocsWithWord+1);
          double tf = Math.log(tf_inter + 1);
          double idf = Math.log(idf_inter);

          // Emit the TF-ICF values into the Map and the cleanup which runs once in the end will emit the values to the context object.
          this.tficfMap.put(new Text(val+"@"+key.toString()), new Text(Double.toString(tf*idf))); // this.Valueword);
      }
		}

		// sorts the output (key,value) pairs that are contained in the tficfMap
		protected void cleanup(Context context) throws IOException, InterruptedException {
            Map<Text, Text> sortedMap = new TreeMap<Text, Text>(tficfMap);
			for (Text key : sortedMap.keySet()) {
                context.write(key, sortedMap.get(key));
            }
        }

    }
}
