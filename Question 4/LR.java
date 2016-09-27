//Team Candidate 1: Priyanka Singh (psingh28)
//Team Candidate 2: Sahil Dureja (sahildur)


import java.io.IOException;
import java.io.DataOutput;
import java.io.DataInput;
import java.util.*;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LR{

	private static class myWritable implements WritableComparable<myWritable> {

		String keyfirst;
		String keysecond;
		//String keythird;

		public void set(String a,String b) throws IOException {
			this.keyfirst=a;
			this.keysecond=b;
			//this.keythird=c;
		}
		public String toString() {

			return (new StringBuilder().append(keyfirst).append("\t")
				.append(keysecond).toString());
		}

		public void readFields(DataInput in) throws IOException {
			keyfirst = in.readUTF();
			keysecond = in.readUTF();
			//keythird = in.readUTF();
			//System.out.println(keysecond);
		}
		public void write(DataOutput out) throws IOException {

			out.writeUTF(keyfirst);
			out.writeUTF(keysecond);
			//out.writeUTF(keythird);

		}
		public int compareTo(myWritable pop) {
			if (pop == null)
			return 0;
			int intcnt = keysecond.compareTo(pop.keysecond);
			return intcnt == 0 ? keyfirst.compareTo(pop.keyfirst) : intcnt;
		}
		public boolean equals(Object obj) {
			myWritable o=(myWritable)obj;

			if(keyfirst.equals(o.keyfirst) && keysecond.equals(o.keysecond))
			{return true;}
			else{
			return false;}

		}
	}

	///// mapper 1
	///// output: year_dept, enrol
	public static class Mapper1 
	extends Mapper<Object,Text,myWritable, IntWritable>{

		private myWritable mykeyy =new myWritable();


		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			try{
			String line = value.toString();
			String[] tokens=line.split(",");

			String x = tokens[3].toString();
			String[] yrsplit=x.split(" ");
			String year = yrsplit[1].trim();

			String depart = tokens[4].toString();
			String enrol = tokens[9].toString();
			mykeyy.set(year,depart);
			int yr=Integer.parseInt(year);
			if(yr>=2006 && yr<=2015)
			{
				int onne = Integer.parseInt(enrol);
				IntWritable val1 = new IntWritable(onne);
				context.write(mykeyy,val1);
			}else
			{return;}
			}
			catch(NumberFormatException e){
				
				//System.out.println(e.getMessage());
			}
			catch(Exception e)
			{return;}
		}
	}

	///// reducer 1
	///// output: year_dept, total enrol
	public static class Reducer1
	extends Reducer<myWritable, IntWritable, myWritable, IntWritable>{
		private IntWritable result = new IntWritable();
		public void reduce(myWritable key, Iterable<IntWritable> values, Context context) throws IOException, 						InterruptedException{
			int sum=0;

			for(IntWritable val:values){
				sum+=val.get();
			}
			result.set(sum);
			context.write(key, result);
		}

	}

	///// mapper 2
	///// output: dept, enrol
	public static class Mapper2 
	extends Mapper<Object,Text,Text,Text>{

		private Text word =new Text();
		private Text val =new Text();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			
			try{
			String line = value.toString();
			String[] tokens=line.split("\\t");
			String year=tokens[0].trim();
			String dept=tokens[1].trim();
			String enrol=tokens[2].trim();
			word.set(dept);
			String temp1=year+"_"+enrol;
			val.set(temp1);
			//int cap = Integer.parseInt(tokens[2].trim());
			//System.out.println(cap);
			//IntWritable val = new IntWritable(cap);
			context.write(word,val);}
			catch(NumberFormatException e){
				System.out.println(e.getMessage());
			}
			catch(Exception e){
				System.out.println(e.getMessage());
			}
		}
	}

	///// reducer 2
	///// output: dept, enrol 
	public static class Reducer2
	extends Reducer<Text, Text, Text, IntWritable>{
		private IntWritable result = new IntWritable();
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, 						InterruptedException{

			List<Text> cache = new ArrayList<Text>();

			int sum=0;
			double count=0.0;
			//System.out.println("loop1");
			//int temp=0;
			for(Text val:values){
				//temp=val.get();
				cache.add(val);
String y=val.toString();
String[] z=y.split("_");
int temp=Integer.parseInt(z[1]);
				//System.out.println("loop1in");
				//System.out.println(temp);
				sum+=temp;
				count++;
			}

			double ave=(double)(sum/count);
			//System.out.println("details");
			System.out.println(ave);

			if(count<10){return;}
			double diffy=0.0;
			double diffx=0.0;
			//int diff_sum=0;
			double xdash=2010.5;
			//double county=2005.0;
			double summerup=0.0;
			double summerdown=0.0;
			//System.out.println("loop2");
			String k=key.toString();
			for(Text vals:cache) {
				
String a= vals.toString();
String[] b=a.split("_");
int year = Integer.parseInt(b[0]);
int enrol = Integer.parseInt(b[1]);

				//System.out.println("loop2in");
				//System.out.println(vals);
				//county++;
				//String keyyy=k+"\t"+(int)county;	
				//context.write(new Text(keyyy),new IntWritable(vals));			
				diffy=(double)(enrol-ave);
				//System.out.println(diffy);
				diffx=year-xdash;
				//System.out.println(diffx);
				summerup+=diffx*diffy;
				//System.out.println(summerup);
				summerdown+=diffx*diffx;
				//System.out.println(summerdown);

			}

			//System.out.println("loopend");
			double b1=summerup/summerdown;
			//System.out.println(b1);
			double b0=ave-(xdash*b1);
			//System.out.println(b0);
			double pred2016=b1*2016.0+b0;
			double pred2017=b1*2017.0+b0;
			double pred2018=b1*2018.0+b0;
			double pred2019=b1*2019.0+b0;
			double pred2020=b1*2020.0+b0;
int county=2015;
			//System.out.println(pred2016);
			String keyyy1=k+"\t"+(int)(++county);	
			context.write(new Text(keyyy1),new IntWritable((int)pred2016));
			keyyy1=k+"\t"+(int)(++county);	
			context.write(new Text(keyyy1),new IntWritable((int)pred2017));
			keyyy1=k+"\t"+(int)(++county);	
			context.write(new Text(keyyy1),new IntWritable((int)pred2018));
			keyyy1=k+"\t"+(int)(++county);	
			context.write(new Text(keyyy1),new IntWritable((int)pred2019));
			keyyy1=k+"\t"+(int)(++county);	
			context.write(new Text(keyyy1),new IntWritable((int)pred2020));


		//result.set((int)pred2016);
				//context.write(key, result);



			
		}

	}


	public static void main(String[] args) throws Exception{
		Configuration conf =new Configuration();
		Job job=Job.getInstance(conf, "enrolment of every dept");
		job.setJarByClass(LR.class);
		job.setMapperClass(Mapper1.class);
		job.setCombinerClass(Reducer1.class);
		job.setReducerClass(Reducer1.class);
		job.setOutputKeyClass(myWritable.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
		//System.out.println("Reducer1 output");
		//System.exit(job.waitForCompletion(true)?0:1);

		Configuration conf2 =new Configuration();
		Job job2=Job.getInstance(conf2, "diff from average");
		job2.setJarByClass(LR.class);
		job2.setMapperClass(Mapper2.class);
		//job2.setCombinerClass(Reducer2.class);
		job2.setReducerClass(Reducer2.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job2, new Path(args[1]));
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));
		//job.waitForCompletion(true);
		//System.out.println("Reducer1 output");
		System.exit(job2.waitForCompletion(true)?0:1);
	}	


}
