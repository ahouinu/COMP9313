/*
 * COMP9313 Big Data 18s2
 * Assignment 1 TF-IDF Inverted Index
 * @Author: Tianpeng Chen
 * @zid: z5176343
 * */


package comp9313.proj1;

import java.io.IOException;
import java.util.HashMap;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Map.Entry;
import java.lang.Math;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.WritableComparable;
//import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Project1 {
	
	/*
	 * KEYIN:	Object
	 * VALUEIN:	Text
	 * KEYOUT:	TFPair (WritableComparable)
	 * VALUEOUT:IntWritable
	 */
	public static class TokenizerMapper extends Mapper<Object, Text, TFPair, IntWritable> {
		
		// set user-defined counter
		public enum DocCounter {
			TOTAL_DOCUMENTS
		}
		
		private IntWritable count = new IntWritable();
		private TFPair pair = new TFPair();
		
		private static HashMap<TFPair, Integer> map = new HashMap<TFPair, Integer>();
		
		/* 
		 * read input file, for each term appearing in each doc
		 * store < <term, docID> , count > into a HashMap
		 * 
		 * By doing this, we can write the total count for each <term, id> pair
		 * into the context after in cleanup(), which also reduces the number of
		 * IO operations
		 * 
		 * */
		public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(), " *$&#/\t\n\f\"'\\,.:;?![](){}<>~-_");
			// get docID (assume it's always the first token in a line)
			String docID = itr.nextToken();
			
			while (itr.hasMoreTokens()) {
				String term = itr.nextToken().toLowerCase();
				TFPair tfPair = new TFPair(term, docID);
				// omit < <term, docID>, tf > to the hashmap
				if (map.containsKey(tfPair)) {
					map.put(tfPair, map.get(tfPair) + 1);
				} else {
					// if the <term, id> appears the first time
					// omit special k-v pair < <term, "-1">, df> to the hashmap
					// which stores term's document frequency
					map.put(tfPair, 1);
					TFPair dfPair = new TFPair(term, "-1");
					if (map.containsKey(dfPair)) {
						map.put(dfPair, map.get(dfPair) + 1);
					} else {
						map.put(dfPair, 1);
					}
				}
			}
			// when this doc is processed, increase TOTAL_DOCUMENTS counter by 1
			// which stores the number of documents in total
			context.getCounter(DocCounter.TOTAL_DOCUMENTS).increment(1);
		}
		
		/*
		 * after all map jobs are finished, cleanup the hashmap, and write to context
		 * 
		 * also omit a special k-v pair < <term, "-2">, N > to reducer, which indicates
		 * the total number of documents
		 * 
		 * in this way, for each term, the reducer will receive 2 special k-v pairs, which are
		 * < <term, "-2">, N > ---- N: total documents
		 * < <term, "-1">, df> ---- document frequency for this term
		 * */
		public void cleanup(Context context)
			throws IOException, InterruptedException {
			Set<Entry<TFPair, Integer>> sets = map.entrySet();
			for(Entry<TFPair, Integer> entry: sets) {
				pair.set(entry.getKey());
				count.set(entry.getValue());
				context.write(pair, count);
				// get totalDocuments from counter
				long mapperTotalDocuments = context.getCounter(DocCounter.TOTAL_DOCUMENTS).getValue();
				String term = entry.getKey().getTerm();
				TFPair tdPair = new TFPair(term, "-2");
				count.set( (int) mapperTotalDocuments);
				context.write(tdPair, count);
			}
		}
	}
	
	
	public static class TermPartitioner extends Partitioner<TFPair, IntWritable> {
		
		/*
		 * to ensure every k-v pair containing the same term will be sent to the same reducer
		 */
		@Override
		public int getPartition(TFPair key, IntWritable value, int numPartitions) {
			// secondary sort
			return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
		}
	}
	
//	@deprecated
//	public static class TermGroupingComparator extends WritableComparator {
//		
//		protected TermGroupingComparator() {
//			super(TFPair.class, true);
//		}
//		
//		// group by term
//		@Override
//		public int compare(WritableComparable wc1, WritableComparable wc2) {
//			TFPair p1 = (TFPair) wc1;
//			TFPair p2 = (TFPair) wc2;
//			return p1.getTerm().compareTo(p2.getTerm());
//		}
//	}
	
	/*
	 * KEYIN:	TFPair < <term, docID>, count >
	 * VALUEIN:	[IntWritable]
	 * KEYOUT:	Text (term)
	 * VALUEOUT:Text (docID,tf-idf)
	 */
	public static class TFIDFReducer extends Reducer<TFPair, IntWritable, Text, Text> {

		// use global variables to store N and DF for each term
		// since every k-v pairs containing the same term will go into the same reducer
		// with N and DF k-vs coming first and second,
		// we can compute N and DF for this term and store them as global variables
		// afterwards when the normal k-v pairs arrive, we can directly calculate its
		// tf-idf weight
		public static long totalDocuments = 0;
		public static int termDocFreq = 0;
		
		private static Text term = new Text();
		private static Text docIDAndWeight = new Text();
		
		public void reduce(TFPair key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
			long tf = 0;
			String currTerm = key.getTerm();
			String currDocID = key.getDocID();
			
			// get special key-value pairs first, 
			// mapper omitted "-1" for df and "-2" for totalDocuments
			if (currDocID.equals("-2")) {
				// get N
				// Note: for the same term, N always comes first, then DF
				// when a new N record comes in, it means a series of new terms are coming
				// we need to reset DF
				for (IntWritable val: values) {
					totalDocuments = val.get();
					// reset DF
					termDocFreq = 0;
				}
				
			} else if (currDocID.equals("-1")) {
				// sum up df
				// DF records always come in the second after N
				// so it's always initialised to 0 before we add values
				for (IntWritable val: values) {
					termDocFreq += val.get();
				}
			} else {
				// sum up tf
				for (IntWritable val: values) {
					tf = val.get();
				}
				// Now we've got N and DF for this term
				// for each < <term, docID>, tf > pair, we can compute its tf-idf weight
				double tfidf = tf * Math.log10((double) totalDocuments / (double) termDocFreq);
				// write out to context
				term.set(currTerm);
				docIDAndWeight.set(currDocID + "," + tfidf);
				context.write(term, docIDAndWeight);
			}
		}
		
		// @note
		// reducers have different context from mappers', thus cannot get mappers' counter
		// if we divide this problem into different jobs, it can be done by writing into job's configuration

//		@deprecated
//		protected void setup(Context context)
//			throws IOException, InterruptedException {
//			super.setup(context);
////			this.totalDocuments = conf.getLong("TOTAL_DOCUMENTS", -1);
////			this.totalDocuments = context.getCounter(DocCounter.TOTAL_DOCUMENTS).getValue();
////			Counters counters = new Counters();
//			this.totalDocuments = 0;
//			this.df = 0;
//			System.out.println("After setup counter is " + totalDocuments);
//		}
	}
	
	
	public static void main(String[] args) 
			throws Exception {
		Configuration conf = new Configuration();
//		conf.setLong("TOTAL_DOCUMENTS", value);
		Job job = Job.getInstance(conf, "Project 1 tf-idf");
		
		job.setJarByClass(Project1.class);
		job.setNumReduceTasks(Integer.parseInt(args[2]));
		
		job.setMapperClass(TokenizerMapper.class);
		job.setPartitionerClass(TermPartitioner.class);
//		job.setGroupingComparatorClass(TermGroupingComparator.class);
//		job.setCombinerClass(TFIDFReducer.class);
		job.setReducerClass(TFIDFReducer.class);
		
		job.setMapOutputKeyClass(TFPair.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
}
