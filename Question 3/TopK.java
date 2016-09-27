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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.WritableComparator;
import java.nio.ByteBuffer;

public class TopK{
	
	
	// First mapper 
	// output: sem_course, enrol 

	public static class Mapper1 
	extends Mapper<Object,Text,Text, IntWritable>{

		private Text word =new Text();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			
			try{
			String line = value.toString();
			String[] tokens=line.split(",");
			
			int semid=Integer.parseInt(tokens[0]);
			String course=tokens[6];
			String sem = tokens[1].toString();
			if(semid<2169){
			String k = sem+"_"+course;
			word.set(k);
			int cap = Integer.parseInt(tokens[7]);
			if(cap>=0){
			IntWritable val = new IntWritable(cap);

			context.write(word, val);}}}
			catch(NumberFormatException e){
				//System.out.println(e.getMessage());
				}
		}
	}

	// first reducer
	// output: sem_course, total enrol
	public static class Reducer1
	extends Reducer<Text, IntWritable, Text, IntWritable>{
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			int sum=0;
			for(IntWritable val:values){
				sum+=val.get();
			}

			result.set(sum);
			context.write(key, result);
		}

	}

	// second mapper
	// output: course, total enrol
	public static class Mapper2 
	extends Mapper<Object,Text,Text, IntWritable>{

		private Text word =new Text();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			
			try{
			String line = value.toString();
			//System.out.println(line);
			String[] tokens=line.split("\\t");
			
			String sem_course=tokens[0].toString();
			String[] keys=sem_course.split("_");
			String course = keys[1].toString();
			//System.out.println(course);
			word.set(course);
			int cap = Integer.parseInt(tokens[1]);
			IntWritable val = new IntWritable(cap);
			context.write(word, val);}
			catch(NumberFormatException e){
				System.out.println(e.getMessage());
				}
		}
	}

	// second reducer
	// output: course, average
	public static class Reducer2
	extends Reducer<Text, IntWritable, Text, IntWritable>{
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			int count=0;
			int sum=0;
			for(IntWritable val:values){
				sum+=val.get();
				count++;
			}
			System.out.println("details");
			System.out.println(count);
			int ave=0;
			try{
			ave=sum/count;}catch(ArithmeticException e){System.out.println(e.getMessage());}
			//System.out.println(sum);
			//System.out.println(ave);
			result.set(ave);
			context.write(key, result);
		}

	}

	// third mapper
	// output: average, course sorted
	public static class Mapper3 
	extends Mapper<Object,Text,IntWritable, Text>{

		private Text word =new Text();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			
			try{
			String line = value.toString();
			String[] tokens=line.split("\\t");
			
			String course=tokens[0].toString();
			word.set(course);
			int cap = Integer.parseInt(tokens[1]);
			IntWritable val = new IntWritable(cap);
			context.write(val,word);}
			catch(NumberFormatException e){
				System.out.println(e.getMessage());
				}
		}
	}

	public static class IntComparator extends WritableComparator {

	    public IntComparator() {
		super(IntWritable.class);
	    }

	    @Override
	    public int compare(byte[] b1, int s1, int l1,
		    byte[] b2, int s2, int l2) {

		Integer v1 = ByteBuffer.wrap(b1, s1, l1).getInt();
		Integer v2 = ByteBuffer.wrap(b2, s2, l2).getInt();

		return v1.compareTo(v2) * (-1);
	    }
}

	// third reducer
	// output: course, average 
	public static class Reducer3
	extends Reducer<IntWritable, Text, Text, IntWritable>{
		private IntWritable result = new IntWritable();
		int counter=0;

		public void reduce(IntWritable key, Text values, Context context) throws IOException, InterruptedException{
			counter++;
			if(counter<21){
			context.write(values, key);}
		}

	}


	public static void main(String[] args) throws Exception{
		Configuration conf =new Configuration();
		Job job=Job.getInstance(conf, "enrolment per semester");
		job.setJarByClass(TopK.class);
		job.setMapperClass(Mapper1.class);
		job.setCombinerClass(Reducer1.class);
		job.setReducerClass(Reducer1.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);


		Configuration conf2 =new Configuration();
		Job job2=Job.getInstance(conf2, "average");
		job2.setJarByClass(TopK.class);
		job2.setMapperClass(Mapper2.class);
		//job2.setCombinerClass(Reducer2.class);
		job2.setReducerClass(Reducer2.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job2, new Path(args[1]));
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));
		job2.waitForCompletion(true);

		
		Configuration conf3 =new Configuration();
		Job job3=Job.getInstance(conf3, "sorting in descending order");
		job3.setJarByClass(TopK.class);
		job3.setMapperClass(Mapper3.class);
		job3.setCombinerClass(Reducer3.class);
		job3.setReducerClass(Reducer3.class);
		job3.setMapOutputKeyClass(IntWritable.class);
		job3.setMapOutputValueClass(Text.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(IntWritable.class);
		job3.setSortComparatorClass(IntComparator.class);
		FileInputFormat.addInputPath(job3, new Path(args[2]));
		FileOutputFormat.setOutputPath(job3, new Path(args[3]));
		System.exit(job3.waitForCompletion(true)?0:1);


	}
}

