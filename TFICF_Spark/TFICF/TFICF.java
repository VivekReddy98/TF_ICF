/*
Single Author Info
vkarri Vivek Reddy Karri
*/

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.*;

/*
 * Main class of the TFICF Spark implementation.
 * Author: Tyler Stocksdale
 * Date:   10/31/2017
 */
public class TFICF {

	static boolean DEBUG = false;

    public static void main(String[] args) throws Exception {
        // Check for correct usage
        if (args.length != 1) {
            System.err.println("Usage: TFICF <input dir>");
            System.exit(1);
        }

		// Create a Java Spark Context
		SparkConf conf = new SparkConf().setAppName("TFICF");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Load our input data
		// Output is: ( filePath , fileContents ) for each file in inputPath
		String inputPath = args[0];
		JavaPairRDD<String,String> filesRDD = sc.wholeTextFiles(inputPath);

		// Get/set the number of documents (to be used in the ICF job)
		long numDocs = filesRDD.count();

		//Print filesRDD contents
		if (DEBUG) {
			List<Tuple2<String, String>> list = filesRDD.collect();
			System.out.println("------Contents of filesRDD------");
			for (Tuple2<String, String> tuple : list) {
				System.out.println("(" + tuple._1 + ") , (" + tuple._2.trim() + ")");
			}
			System.out.println("--------------------------------");
		}

		/*
		 * Initial Job
		 * Creates initial JavaPairRDD from filesRDD
		 * Contains each word@document from the corpus and also attaches the document size for
		 * later use
		 *
		 * Input:  ( filePath , fileContents )
		 * Map:    ( (word@document) , docSize )
		 */
		JavaPairRDD<String,Integer> wordsRDD = filesRDD.flatMapToPair(
			new PairFlatMapFunction<Tuple2<String,String>,String,Integer>() {
				public Iterable<Tuple2<String,Integer>> call(Tuple2<String,String> x) {
					// Collect data attributes
					String[] filePath = x._1.split("/");
					String document = filePath[filePath.length-1];
					String fileContents = x._2;
					String[] words = fileContents.split("\\s+");
					int docSize = words.length;

					// Output to Arraylist
					ArrayList ret = new ArrayList();
					for(String word : words) {
						ret.add(new Tuple2(word.trim() + "@" + document, docSize));
					}
					return ret;
				}
			}
		);

		//Print wordsRDD contents
		if (DEBUG) {
			List<Tuple2<String, Integer>> list = wordsRDD.collect();
			System.out.println("------Contents of wordsRDD------");
			for (Tuple2<String, Integer> tuple : list) {
				System.out.println("(" + tuple._1 + ") , (" + tuple._2 + ")");
			}
			System.out.println("--------------------------------");
		}

		/*
		 * TF Job (Word Count Job + Document Size Job)
		 * Gathers all data needed for TF calculation from wordsRDD
		 *
		 * Input:  ( (word@document) , docSize )
		 * Map:    ( (word@document) , (1/docSize) )
		 * Reduce: ( (word@document) , (wordCount/docSize) )
		 */

			JavaPairRDD<String,String> tfRDD = wordsRDD.groupByKey().mapValues(
			 new Function<Iterable<Integer>, String>() {
					public String call(Iterable<Integer> x) {

						 // Variable to count Wordcount
						 int wordCount = 0;

						 //Varible to store docSize
						 int res = 0;

		    		 for (Integer val : x) {
							 res = val.intValue();
		    		   wordCount += 1; // Increment WordCount
		    		 }

						 //returns the string in the format (wordCount/docSize)
						 return Integer.toString(wordCount)+"/"+Integer.toString(res);
					}
				}
			);

			//Print tfRDD contents
			if (DEBUG) {
				List<Tuple2<String, String>> list = tfRDD.collect();
				System.out.println("-------Contents of tfRDD--------");
				for (Tuple2<String, String> tuple : list) {
					System.out.println("(" + tuple._1 + ") , (" + tuple._2 + ")");
				}
				System.out.println("--------------------------------");
			}

		/*
		 * ICF Job
		 * Gathers all data needed for ICF calculation from tfRDD
		 *
		 * Input:  ( (word@document) , (wordCount/docSize) )
		 * Map:    ( word , (1/document) )
		 * Reduce: ( word , (numDocsWithWord/document1,document2...) )
		 * Map:    ( (word@document) , (numDocs/numDocsWithWord) )
		 */

		JavaPairRDD<String,String> icfRDD = tfRDD.flatMapToPair(
			new PairFlatMapFunction<Tuple2<String,String>,String,String>() {
				public Iterable<Tuple2<String,String>> call(Tuple2<String,String> x) {
					// Split word@document into [word, document]
					String[] wordAndDoc = x._1.split("@");

					// Output to Arraylist
					ArrayList ret = new ArrayList();

				  // Return a tuple of (document, word)
					ret.add(new Tuple2(wordAndDoc[0], wordAndDoc[1]));
					return ret;
				}
			}
		).groupByKey().flatMapToPair(
			new PairFlatMapFunction<Tuple2<String,Iterable<String>>,String,String>() {
				public Iterable<Tuple2<String,String>> call(Tuple2<String,Iterable<String>> x) {

					// Variable to store numDocsWithWord information
					int numDocsWithWord = 0;

					// x._1 gives the first value in the tuple i.e word
					String word = x._1;
					ArrayList<String> docs = new ArrayList<String>();

					// Loop over all the Iterable and compute totalWords and while compouting also store document information
					for (String doc : x._2){
              numDocsWithWord += 1;
							docs.add(doc);
          }

					// Iterator to loop through the ArrayList
					Iterator i = docs.iterator();
          ArrayList ret = new ArrayList();

					// Loop through the Array List and generate the corresponding output type.
          while (i.hasNext()) {
						   String doc = (String)i.next();
            	 ret.add(new Tuple2(word+"@"+doc, Long.toString(numDocs)+"/"+Long.toString(numDocsWithWord)));
          }
					return ret;
				}
			}
		);

		//Print icfRDD contents
		if (DEBUG) {
			List<Tuple2<String, String>> list = icfRDD.collect();
			System.out.println("-------Contents of icfRDD-------");
			for (Tuple2<String, String> tuple : list) {
				System.out.println("(" + tuple._1 + ") , (" + tuple._2 + ")");
			}
			System.out.println("--------------------------------");
		}

		/*
		 * TF * ICF Job
		 * Calculates final TFICF value from tfRDD and icfRDD
		 *
		 * Input:  ( (word@document) , (wordCount/docSize) )          [from tfRDD]
		 * Map:    ( (word@document) , TF )
		 *
		 * Input:  ( (word@document) , (numDocs/numDocsWithWord) )    [from icfRDD]
		 * Map:    ( (word@document) , ICF )
		 *
		 * Union:  ( (word@document) , TF )  U  ( (word@document) , ICF )
		 * Reduce: ( (word@document) , TFICF )
		 * Map:    ( (document@word) , TFICF )
		 *
		 * where TF    = log( wordCount/docSize + 1 )
		 * where ICF   = log( (Total numDocs in the corpus + 1) / (numDocsWithWord in the corpus + 1) )
		 * where TFICF = TF * ICF
		 */
		JavaPairRDD<String,Double> tfFinalRDD = tfRDD.mapToPair(
			new PairFunction<Tuple2<String,String>,String,Double>() {
				public Tuple2<String,Double> call(Tuple2<String,String> x) {
					double wordCount = Double.parseDouble(x._2.split("/")[0]);   // Extract WordCount
					double docSize = Double.parseDouble(x._2.split("/")[1]);      // Extract Docsize
					double TF = Math.log(wordCount/docSize + 1);      // Compute TF = log( wordCount/docSize + 1 )
					return new Tuple2(x._1, TF);
				}
			}
		);

		JavaPairRDD<String,Double> icfFinalRDD = icfRDD.mapToPair(
			new PairFunction<Tuple2<String,String>,String,Double>() {
				public Tuple2<String,Double> call(Tuple2<String,String> x) {
					double numDocs = Double.parseDouble(x._2.split("/")[0]);     // Extract numDocs
					double numDocsWithWord = Double.parseDouble(x._2.split("/")[1]);    // Extract numDocsWithWord
					double ICF = Math.log((numDocs+1)/(numDocsWithWord+1));  // Compute ICF = log((Total numDocs in the corpus + 1) / (numDocsWithWord in the corpus + 1))
					return new Tuple2(x._1, ICF);
				}
			}
		);

		// Associative Function Which will reduce any set of values using (_*_) op
		Function2<Double, Double, Double> reduceMultiplyFunc = (tf, icf) -> (tf*icf);

		// Union Creates an Iterable and reduce OP is used to reduce the values by reduceMultiplyFunc op.
		JavaPairRDD<String,Double> tficfRDD = tfFinalRDD.union(icfFinalRDD).reduceByKey(reduceMultiplyFunc).flatMapToPair(
			new PairFlatMapFunction<Tuple2<String,Double>,String,Double>() {

				// Trivial Computation to Convert word@document into document@word
				public Iterable<Tuple2<String,Double>> call(Tuple2<String,Double> x) {

					String[] wordAndDoc = x._1.split("@");

					ArrayList ret = new ArrayList();
					ret.add(new Tuple2(wordAndDoc[1]+"@"+wordAndDoc[0], x._2));
					return ret;
				}
			}
			);

		// System.out.println("-------Contents of RDD-------");
		// tfFinalRDD.foreach((str) -> System.out.println(str));
		// System.out.println("-------Contents of icfRDD-----");
		// icfFinalRDD.foreach((str) -> System.out.println(str));
		// System.out.println("-------Contents of tficfRDD-----");
		// tficfRDD.foreach((str) -> System.out.println(str));

		//Print tficfRDD contents in sorted order
		Map<String, Double> sortedMap = new TreeMap<>();
		List<Tuple2<String, Double>> list = tficfRDD.collect();
		for (Tuple2<String, Double> tuple : list) {
			sortedMap.put(tuple._1, tuple._2);
		}
		if(DEBUG) System.out.println("-------Contents of tficfRDD-------");
		for (String key : sortedMap.keySet()) {
			System.out.println(key + "\t" + sortedMap.get(key));
		}
		if(DEBUG) System.out.println("--------------------------------");

	}
}
