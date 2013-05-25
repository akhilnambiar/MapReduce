import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/*
 * This is the skeleton for CS61c project 1, Fall 2012.
 *
 * Contact Alan Christopher or Ravi Punj with questions and comments.
 *
 * Reminder:  DO NOT SHARE CODE OR ALLOW ANOTHER STUDENT TO READ YOURS.
 * EVEN FOR DEBUGGING. THIS MEANS YOU.
 *
 */
public class Proj1 {

    /** An Example Writable which contains two String Objects. */
    public static class StringPair implements Writable {
    	
        /** The String objects I wrap. */
    	private String a, b;

		/** Initializes me to contain empty strings. */
		public StringPair() {
		    a = b = "";
		}
	
		/** Initializes me to contain A, B. */
        public StringPair(String a, String b) {
            this.a = a;
            this.b = b;
        }

        /** Serializes object - needed for Writable. */
        public void write(DataOutput out) throws IOException {
            new Text(a).write(out);
            new Text(b).write(out);
        }

        /** Deserializes object - needed for Writable. */
        public void readFields(DataInput in) throws IOException {
		    Text tmp = new Text();
		    tmp.readFields(in);
		    a = tmp.toString();
		    
		    tmp.readFields(in);
        }
	
		/** Returns A. */
		public String getA() {
		    return a;
		}
		
		/** Returns B. */
		public String getB() {
		    return b;
		}
    }


  /**
   * Inputs a set of (docID, document contents) pairs.
   * Outputs a set of (Text, Text) pairs.
   */
    public static class Map1 extends Mapper<WritableComparable, Text, Text, StringPair> {
        /** Regex pattern to find words (alphanumeric + _). */
        final static Pattern WORD_PATTERN = Pattern.compile("\\w+");//single word pattern

        private String targetGram = null;
		private int funcNum = 0;

        /*
         * Setup gets called exactly once for each mapper, before map() gets called the first time.
         * It's a good place to do configuration or setup that can be shared across many calls to map
         */
        @Override
        public void setup(Context context) {
            targetGram = context.getConfiguration().get("targetGram").toLowerCase();
			try {
				funcNum = Integer.parseInt(context.getConfiguration().get("funcNum"));
			}
			catch (NumberFormatException e) {
			/* Do nothing. */
			}
        }

        @Override
        public void map(WritableComparable docID, Text docContents, Context context)
                throws IOException, InterruptedException {
            Matcher matcher = WORD_PATTERN.matcher(docContents.toString());
			Func func = funcFromNum(funcNum);
			ArrayList<String> GramArray = new ArrayList<String>();
			while (matcher.find()) //sends the singe word matcher
				{
                GramArray.add(matcher.group().toLowerCase()); //takes the word and adds it; this array will be used to search target grams
				}
			String inputText = docContents.toString().toLowerCase(); //takes the whole input makes it string
			String[] targetGrams = targetGram.split("\\s+");//split to search the array for target grams
			boolean[] locationArray = new boolean[GramArray.size()] ;//same length as all of the target grams 		
			int counter = 0;
				for (int x=0; x<GramArray.size()-targetGrams.length+1; x++) 
					{				
						boolean targetAquire = true;
							for (int i=0;i<targetGrams.length;i++) 
							{											//I FOUND THE BUG!!!!!!!!
								if (!GramArray.get(x+i).equals(targetGrams[i])) //This array will go through each item and check to see if a target gram was found
									{
									targetAquire = false;
									break;
									}
							}
					if (targetAquire) //If the target gram is found
						{
						counter=counter+1;
						locationArray[x] = true; //sets that specific array location as true, marking the left index
						}
					}
				if (counter>0) 
					{
					for (int i=0;i<GramArray.size()-targetGrams.length+1;i++)  //Goes through and counts
						{
						int distance = 0;
							while (true) 
								{
								if (i+distance<GramArray.size() && locationArray[i+distance])
									{
									break;   //break if distance is that location
									}
								if (i-distance>=0 && locationArray[i-distance]) //done with array
									{
									break; // same but checking from the other side
									}
								distance=distance+1;  //or else just add 1
								}
						if (distance != 0) //if a distance does exist
							{
							String NGRAM = "";
								for (int j=0;j<targetGrams.length;j++) //this will make the resulting ngram of length n
									{
									NGRAM += GramArray.get(i+j); //gets that part of the gram
									NGRAM += " ";
									}
							Text GramFinal = new Text(NGRAM.substring(0,NGRAM.length()-1));
							StringPair Dist = new StringPair(Double.toString(func.f(distance)), "1"); //string 1 is used to help count occurances
							context.write(GramFinal, Dist); //VICTORY!!!!
							}
				}
			}
			else 
				{
				for (int i=0; i<GramArray.size()-targetGrams.length+1; i++) 
					{
					String NGRAM = "";
					for (int p=0; p<targetGrams.length; p++) 
						{
						NGRAM += GramArray.get(i+p);
						NGRAM += " ";
						}
						Text GramFinal = new Text(NGRAM.substring(0,NGRAM.length()-1));
						Text FDistance = new Text( Double.toString( func.f(Double.POSITIVE_INFINITY) ));
						StringPair Dist = new StringPair(Double.toString(func.f(Double.POSITIVE_INFINITY)), "1");
						context.write(GramFinal, Dist);
					}
				}
		}

		/** Returns the Func corresponding to FUNCNUM*/
		private Func funcFromNum(int funcNum) {
			Func func = null;
			switch (funcNum) {
			case 0:	
			func = new Func() {
				public double f(double d) {
					return d == Double.POSITIVE_INFINITY ? 0.0 : 1.0;
				}			
				};	
			break;
			case 1:
			func = new Func() {
				public double f(double d) {
					return d == Double.POSITIVE_INFINITY ? 0.0 : 1.0 + 1.0 / d;
				}			
				};
			break;
			case 2:
			func = new Func() {
				public double f(double d) {
					return d == Double.POSITIVE_INFINITY ? 0.0 : Math.sqrt(d);
				}			
				};
			break;
			}
			return func;
		}
    }

    /** Here's where you'll be implementing your combiner. It must be non-trivial for you to receive credit. */
    public static class Combine1 extends Reducer<Text, StringPair, Text, StringPair> {   //Change this to string pair

      @Override
      public void reduce(Text key, Iterable<StringPair> values,
              Context context) throws IOException, InterruptedException {
					double sum = 0;
					int counter = 0;
					for (StringPair s : values) 
						{
						sum += Double.parseDouble(s.getA());  //adds the first value of string pair
						counter += Integer.parseInt(s.getB()); //adds second value of the string pair
						}
				StringPair value = new StringPair(sum+"", counter+"");
				context.write(key, value);
      }
    }


    public static class Reduce1 extends Reducer<Text, StringPair, DoubleWritable, Text> {
        @Override
        public void reduce(Text key, Iterable<StringPair> values,
			   Context context) throws IOException, InterruptedException {
				// Apply function to values, and output.
				double sum =0;
				int GRAMS =0;
				for (StringPair value: values) 
					{
					sum += Double.parseDouble(value.getA());
					GRAMS += Double.parseDouble(value.getB());      //Print statements here
					}
					double coocurrance;
					if (sum>0) 
						{
						coocurrance = sum*Math.pow(Math.log(sum),3)/Math.abs(GRAMS);
						}
					else 
						{
						coocurrance=0;
						}
				DoubleWritable rate = new DoubleWritable(coocurrance);
				context.write(rate, key);
        }
    }

    public static class Map2 extends Mapper<DoubleWritable, Text, DoubleWritable, Text> {
		//maybe do something, maybe don't
		@Override
		public void map(DoubleWritable key, Text value, Context context)
		throws IOException, InterruptedException {
			key.set(-1*key.get());					//all of the output values will be the opposite now
			context.write(key, value);
		}
    }

    public static class Reduce2 extends Reducer<DoubleWritable, Text, DoubleWritable, Text> {

      int n = 0;
      static int N_TO_OUTPUT = 100;

      /*
       * Setup gets called exactly once for each reducer, before reduce() gets called the first time.
       * It's a good place to do configuration or setup that can be shared across many calls to reduce
       */
      @Override
      protected void setup(Context c) {
        n = 0;
      }

        @Override
        public void reduce(DoubleWritable key, Iterable<Text> values,
                Context context) throws IOException, InterruptedException {
			int counter = N_TO_OUTPUT;
			for(Text value: values)
				{
				if (counter>0)
					{
					if (Double.compare(key.get(), 0.0) < 0) 
						{
						key.set(-1*key.get());
						}
					context.write(key, value);
				}
				counter=counter-1;
			}
        }
    }

    /*
     *  You shouldn't need to modify this function much. If you think you have a good reason to,
     *  you might want to discuss with staff.
     *
     *  The skeleton supports several options.
     *  if you set runJob2 to false, only the first job will run and output will be
     *  in TextFile format, instead of SequenceFile. This is intended as a debugging aid.
     *
     *  If you set combiner to false, neither combiner will run. This is also
     *  intended as a debugging aid. Turning on and off the combiner shouldn't alter
     *  your results. Since the framework doesn't make promises about when it'll
     *  invoke combiners, it's an error to assume anything about how many times
     *  values will be combined.
     */
    public static void main(String[] rawArgs) throws Exception {
        GenericOptionsParser parser = new GenericOptionsParser(rawArgs);
        Configuration conf = parser.getConfiguration();
        String[] args = parser.getRemainingArgs();

        boolean runJob2 = conf.getBoolean("runJob2", true);
        boolean combiner = conf.getBoolean("combiner", false);

        if(runJob2)
          System.out.println("running both jobs");
        else
          System.out.println("for debugging, only running job 1");

        if(combiner)
          System.out.println("using combiner");
        else
          System.out.println("NOT using combiner");

        Path inputPath = new Path(args[0]);
        Path middleOut = new Path(args[1]);
        Path finalOut = new Path(args[2]);
        FileSystem hdfs = middleOut.getFileSystem(conf);
        int reduceCount = conf.getInt("reduces", 32);

        if(hdfs.exists(middleOut)) {
          System.err.println("can't run: " + middleOut.toUri().toString() + " already exists");
          System.exit(1);
        }
        if(finalOut.getFileSystem(conf).exists(finalOut) ) {
          System.err.println("can't run: " + finalOut.toUri().toString() + " already exists");
          System.exit(1);
        }

        {
            Job firstJob = new Job(conf, "wordcount+co-occur");

            firstJob.setJarByClass(Map1.class);

	    /* You may need to change things here */
            firstJob.setMapOutputKeyClass(Text.class);
            firstJob.setMapOutputValueClass(StringPair.class);  //This was changed is this write??
            firstJob.setOutputKeyClass(DoubleWritable.class);
            firstJob.setOutputValueClass(Text.class);
	    /* End region where we expect you to perhaps need to change things. */

            firstJob.setMapperClass(Map1.class);
            firstJob.setReducerClass(Reduce1.class);
            firstJob.setNumReduceTasks(reduceCount);


            if(combiner)
              firstJob.setCombinerClass(Combine1.class);

            firstJob.setInputFormatClass(SequenceFileInputFormat.class);
            if(runJob2)
              firstJob.setOutputFormatClass(SequenceFileOutputFormat.class);

            FileInputFormat.addInputPath(firstJob, inputPath);
            FileOutputFormat.setOutputPath(firstJob, middleOut);

            firstJob.waitForCompletion(true);
        }

        if(runJob2) {
            Job secondJob = new Job(conf, "sort");

            secondJob.setJarByClass(Map1.class);
	    /* You may need to change things here */
            secondJob.setMapOutputKeyClass(DoubleWritable.class);
            secondJob.setMapOutputValueClass(Text.class);
            secondJob.setOutputKeyClass(DoubleWritable.class);
            secondJob.setOutputValueClass(Text.class);
	    /* End region where we expect you to perhaps need to change things. */

            secondJob.setMapperClass(Map2.class);
            if(combiner)
              secondJob.setCombinerClass(Reduce2.class);
            secondJob.setReducerClass(Reduce2.class);

            secondJob.setInputFormatClass(SequenceFileInputFormat.class);
            secondJob.setOutputFormatClass(TextOutputFormat.class);
            secondJob.setNumReduceTasks(1);


            FileInputFormat.addInputPath(secondJob, middleOut);
            FileOutputFormat.setOutputPath(secondJob, finalOut);

            secondJob.waitForCompletion(true);
        }
    }

}
