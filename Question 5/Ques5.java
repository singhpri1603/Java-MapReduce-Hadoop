//Team Candidate 1: Priyanka Singh (psingh28)
//Team Candidate 2: Sahil Dureja (sahildur)


import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Ques5{

	static long enrola;
	static long capa;
	static int i=0;

////
	public static class Mapper3 extends Mapper<Object,Text,Text, Text>{
		private Text keyy =new Text();
		private Text vall =new Text();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			try{
			String line2 = value.toString();
			String[] tokens=line2.split("#");

			String[] tokenskeyval=line2.split("\\t");

			if(tokens[1].equals("counter"))
			{
				keyy.set(tokens[0].trim());
				vall.set(tokenskeyval[1].trim()+"_c1_");
				context.write(keyy,vall);

			}else
			{
				keyy.set(tokens[0].trim());
				vall.set(tokenskeyval[1].trim()+"_c2_");
				context.write(keyy,vall);

			}

			}catch(Exception e)
			{
				System.out.println("exception3");
			}
		}

	}
////
//
	public static class Mapper2 extends Mapper<Object,Text,Text, IntWritable>{
		private Text keyy =new Text();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			try{
			String line2 = value.toString();
			String[] tokens=line2.split("#");

			String[] tokenskeyval=line2.split("\\t");


			int one = Integer.parseInt("1");
						IntWritable val1 = new IntWritable(one);
			int zero = Integer.parseInt("0");
						IntWritable zero1 = new IntWritable(zero);

			if(tokens[2].trim().equals("counter"))
			{
				keyy.set(tokens[0].trim()+"#counter#");
				context.write(keyy,val1);
			}
			else{
				if(Integer.parseInt(tokenskeyval[1])>0)
				{
					keyy.set(tokens[0].trim()+"#enrolabove30#");
					context.write(keyy,val1);

				}else
				{
					keyy.set(tokens[0].trim()+"#enrolabove30#");
					context.write(keyy,zero1);

				}

			}
//
			}catch(Exception e)
				{System.out.println("exception 2");}
		}//map end
	}//mapper2 end

//
	public static class TokenizerMapper 
	extends Mapper<Object,Text,Text, IntWritable>{

		//private final static IntWritable one = new IntWritable(1);
		private Text keyy =new Text();


		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			try{


			String line = value.toString();
			String[] tokens=line.split(",");

			//String x = tokens[2].toString();
			//String[] hall=x.split(" ");
			//String building = hall[0].trim();
			//String k = building+"_"+tokens[2].trim();

			String sem=tokens[1].trim();
			String course=tokens[6].trim();
			String course_enrol=tokens[7].trim();

			if(Integer.parseInt(tokens[0].trim())<1951)
			{return;}

			keyy.set(sem+"#"+course+"#"+"counter#");


			int one = Integer.parseInt("1");
			IntWritable val1 = new IntWritable(one);
			int zero = Integer.parseInt("0");
			IntWritable zero1 = new IntWritable(zero);
			context.write(keyy,val1);

			int ce = Integer.parseInt(course_enrol);
			IntWritable ce1 = new IntWritable(ce);
			keyy.set(sem+"#"+course+"#"+"enrolabove30#");
			if(ce>=30)
			{


			context.write(keyy,val1);
			}else
			{
			context.write(keyy,zero1);
			}

			
			}
			catch(NumberFormatException e){
				
				System.out.println(e.getMessage());
				}
			

			//StringTokenizer itr = new StringTokenizer(value.toString());
			//while(itr.hasMoreTokens()){
				//word.set(itr.nextToken());
				//context.write(word, one);

			
		}
	}
///
	public static class Reducer2
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

///

//////
	public static class Reducer3
	extends Reducer<Text, Text, Text, Text>{
		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			String sum="";
			String l="";
			String r="";

			for(Text val:values){
			String[] tokens=val.toString().split("_");

				if(tokens[1].equals("c1"))
				{
						l=tokens[0];
				}else
				{
				r=tokens[0];
				}
			}

			sum=l+"\t"+r;

			result.set(sum.trim());
			context.write(key, result);

		}

	}
//////



	public static class IntSumReducer
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

	public static void main(String[] args) throws Exception{


		Configuration conf =new Configuration();
		Job job=Job.getInstance(conf, "word count");
		job.setJarByClass(Ques5.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
		System.out.println("Reducer1 output");

		Configuration conf2 =new Configuration();
		Job job2=Job.getInstance(conf2, "word count");
		job2.setJarByClass(Ques5.class);
		job2.setMapperClass(Mapper2.class);
		job2.setCombinerClass(Reducer2.class);
		job2.setReducerClass(Reducer2.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job2, new Path(args[1]));
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));
		job2.waitForCompletion(true);


		System.out.println("Reducer2 output");
		Configuration conf3 =new Configuration();
		Job job3=Job.getInstance(conf3, "word count");
		job3.setJarByClass(Ques5.class);
		job3.setMapperClass(Mapper3.class);
		//job3.setCombinerClass(Reducer3.class);
		job3.setReducerClass(Reducer3.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job3, new Path(args[2]));
		FileOutputFormat.setOutputPath(job3, new Path(args[3]));

		System.out.println("Reducer3 output");
		System.exit(job3.waitForCompletion(true)?0:1);

		

	}
}

