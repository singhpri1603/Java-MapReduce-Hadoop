//Team Candidate 1: Priyanka Singh (psingh28)
//Team Candidate 2: Sahil Dureja (sahildur)


import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.WritableComparator;
import java.nio.ByteBuffer;
//import org.apache.hadoop.mapred.lib.MultipleInputs;



public class NoExam{
	
	
	// First mapper for class schedule
	// output: sem_course, 1 

	public static class Mapper1 
	extends Mapper<Object,Text,Text, IntWritable>{

		private Text word =new Text();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			
			try{
			String line = value.toString();
			String[] tokens=line.split(",");
			
			int semid=Integer.parseInt(tokens[0]);
			String course=tokens[6].trim();
			String sem = tokens[1].trim();
			if(semid>2118 && semid<2162){
			String k = sem+"_"+course;
			word.set(k);
			
			IntWritable val = new IntWritable(1);

			context.write(word, val);}}
			catch(NumberFormatException e){
				//System.out.println(e.getMessage());
				}
		}
	}

	// first reducer
	// output: sem_course, 1
	public static class Reducer1
	extends Reducer<Text, IntWritable, Text, IntWritable>{
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			
			result.set(1);
			context.write(key, result);
		}

	}

	// second mapper for exam schedule
	// output: sem_course, 1
	public static class Mapper2 
	extends Mapper<Object,Text,Text, IntWritable>{

		private Text word =new Text();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			
			try{
			String line = value.toString();
			String[] tokens=line.split("\\t");
			//System.out.println(tokens.length);
			int semid=Integer.parseInt(tokens[0].trim());
			System.out.println(semid);
			String course=tokens[13].trim();
			String sem = tokens[3].trim();
			if(semid>2118 && semid<2162){
			String k = sem+"_"+course;
			System.out.println(k);
			word.set(k);
			
			IntWritable val = new IntWritable(1);

			context.write(word, val);}}
			catch(NumberFormatException e){
				//System.out.println(e.getMessage());
				}
		}
	}

	// second reducer
	// output: sem_course, 1
	public static class Reducer2
	extends Reducer<Text, IntWritable, Text, IntWritable>{
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			result.set(1);
			context.write(key, result);
		}

	}

	// third mapper
	// output: sem_course, 1
	public static class Mapper3 
	extends Mapper<Object,Text,Text,IntWritable>{

		private Text word =new Text();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			
			try{
			String line = value.toString();
			String[] tokens=line.split("\\t");
			
			String course=tokens[0].toString();
			word.set(course);
			
			IntWritable val = new IntWritable(1);
			context.write(word,val);}
			catch(NumberFormatException e){
				System.out.println(e.getMessage());
				}
		}
	}


	// third reducer
	// output: sem_course, 1 
	public static class Reducer3
	extends Reducer<Text, IntWritable, Text, IntWritable>{
		private IntWritable result = new IntWritable();
		int counter=0;

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			int diff=2;
			for(IntWritable val:values){
				diff-=val.get();
			}
			if(diff>0){
			context.write(key,new IntWritable(diff));}
		}

	}


	public static void main(String[] args) throws Exception{
		Configuration conf =new Configuration();
		Job job=Job.getInstance(conf, "courses from class schedule");
		job.setJarByClass(NoExam.class);
		//MultipleInputs.addInputPath(job,new Path(args[0]),TextInputFormat.class, Mapper1.class);
 		//MultipleInputs.addInputPath(job,new Path(args[1]),TextInputFormat.class, Mapper2.class);
		job.setMapperClass(Mapper1.class);
		job.setCombinerClass(Reducer1.class);
		job.setReducerClass(Reducer1.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		job.waitForCompletion(true);


		Configuration conf2 =new Configuration();
		Job job2=Job.getInstance(conf2, "courses from exam schedule");
		job2.setJarByClass(NoExam.class);
		job2.setMapperClass(Mapper2.class);
		job2.setCombinerClass(Reducer1.class);
		job2.setReducerClass(Reducer1.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job2, new Path(args[1]));
		FileOutputFormat.setOutputPath(job2, new Path(args[3]));
		job2.waitForCompletion(true);

		
		Configuration conf3 =new Configuration();
		Job job3=Job.getInstance(conf3, "result");
		job3.setJarByClass(NoExam.class);
		job3.setMapperClass(Mapper3.class);
		MultipleInputs.addInputPath(job3,new Path(args[2]),TextInputFormat.class);
 		MultipleInputs.addInputPath(job3,new Path(args[3]),TextInputFormat.class);
		job3.setCombinerClass(Reducer3.class);
		job3.setReducerClass(Reducer3.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(IntWritable.class);
		//FileInputFormat.addInputPath(job3, new Path(args[2]));
		FileOutputFormat.setOutputPath(job3, new Path(args[4]));
		System.exit(job3.waitForCompletion(true)?0:1);


	}
}

