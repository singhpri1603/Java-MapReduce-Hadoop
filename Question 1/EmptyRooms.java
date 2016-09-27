//Team candidate 1: Priyanka Singh (psingh28)
//Team candidate 2: Sahil Dureja (sahildur)

import java.io.IOException;
import java.io.DataOutput;
import java.io.DataInput;
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

public class EmptyRooms{


	//////Custom writable class/////
	private static class myWritable implements WritableComparable<myWritable> {

		  String keyfirst;
		  String keysecond;
		  String keythird;

  		public void set(String a,String b,String c) throws IOException {
			this.keyfirst=a;
			this.keysecond=b;
			this.keythird=c;
  		}

		public String toString() {

			return (new StringBuilder().append(keyfirst).append("\t")
				.append(keysecond).append("\t").append(keythird).toString());
		}


		public void readFields(DataInput in) throws IOException {
		    keyfirst = in.readUTF();
		    keysecond = in.readUTF();
		    keythird = in.readUTF();
			//System.out.println(keysecond);
		}

		//@Override
		public void write(DataOutput out) throws IOException {
			//System.out.println(keyfirst);

		    out.writeUTF(keyfirst);
		    out.writeUTF(keysecond);
		    out.writeUTF(keythird);

		}

		public int compareTo(myWritable pop) {
			if (pop == null)
				return 0;
			int intcnt = keyfirst.compareTo(pop.keyfirst);
			intcnt= intcnt == 0 ? keysecond.compareTo(pop.keysecond) : intcnt;
			return intcnt == 0 ? keythird.compareTo(pop.keythird) : intcnt;
		}
		
		public boolean equals(Object obj) {
			myWritable o=(myWritable)obj;

			if(keyfirst.equals(o.keyfirst) && keysecond.equals(o.keysecond) && keythird.equals(o.keythird))
			{return true;}
			else{
			return false;}

		}
	}


	////// Mapper 1
	////// output: <Room+day+time, 0/1(in use or not in use)>
	public static class TokenizerMapper 
	extends Mapper<Object,Text,myWritable, IntWritable>{

		//private final static IntWritable one = new IntWritable(1);
//		private Text keyy =new Text();
  		private myWritable mykeyy =new myWritable();
		


		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			try{
			String line = value.toString();
			String[] tokens=line.split(",");

			String x = tokens[2].toString();
			String[] hall=x.split(" ");
			
			String sem=tokens[1];
			String keyday = tokens[3].toString().trim();
			String keytime = tokens[4].toString().trim();

			if(x.equals("Unknown") || x.equals("Arr Arr") || keyday.equals("UNKWN") || keytime.equals("Unknown")){return;}

			if(sem.equals("Spring 2016"))
			{

				String[] days = new String[5];
				 
				    days[0] = "M";
				    days[1] = "T";
				    days[2] = "W";
				    days[3] = "R";
				    days[4] = "F";

				String[] times=new String[12];
				     times[0] = "10:00AM - 10:59AM";
				     times[1] = "11:00AM - 11:59AM";
				     times[2] = "12:00PM - 12:59PM";
				     times[3] = "1:00PM - 1:59PM";
				     times[4] = "2:00PM - 2:59PM";
				     times[5] = "3:00PM - 3:59PM";
				     times[6] = "4:00PM - 4:59PM";
				     times[7] = "5:00PM - 5:59PM";
				     times[8] = "6:00PM - 6:59PM";
				     times[9] = "7:00PM - 7:59PM";
				     times[10] = "8:00PM - 8:59PM";
				     times[11] = "9:00PM - 9:59PM";

				int empt = Integer.parseInt("0");
				IntWritable empt1 = new IntWritable(empt);

				for(String t:times)
				{

					for(String d: days)
					{
						//System.out.println(x+" "+d+" "+t+" ");
						mykeyy.set(x,d,t);

						context.write(mykeyy,empt1);			
					}
				}

			

				int fille = Integer.parseInt("1");
				IntWritable fille1 = new IntWritable(fille);

				char[] inchar=keyday.toCharArray();
				int enrol = Integer.parseInt(tokens[7].trim());
				IntWritable val1 = new IntWritable(enrol);
				boolean hyphen=false;

				for(char c:inchar)
				{
					if(c=='-')
					{
						hyphen=true;
						break;
					}
				}


				if(hyphen==true)
				{
					String[] daysa = new String[5];
 
					    days[0] = "M";
					    days[1] = "T";
					    days[2] = "W";
					    days[3] = "R";
					    days[4] = "F";
					for(String dd:daysa)
					{
						mykeyy.set(x,dd,keytime);
						context.write(mykeyy,fille1);
					}
				}
				else
				{
					for(char cc:inchar)
					{
						mykeyy.set(x,cc+"",keytime);
						context.write(mykeyy,fille1);
					}

				}
			}//ifspring2016
			
			}
			catch(NumberFormatException e){
				
				System.out.println(e.getMessage());
				}
			catch(Exception e)
			{
			return;}
			

		}
	}

	///// Reducer 1
	///// OUtput: <room+day+time, a number(for separate rows of grads and undergrads)>

	public static class IntSumReducer
				extends Reducer<myWritable, IntWritable, myWritable, IntWritable>{
		private IntWritable result = new IntWritable();

		public void reduce(myWritable key, Iterable<IntWritable> values, Context context) throws IOException, 								InterruptedException{
			int sum=0;
			for(IntWritable val:values){
				sum+=val.get();
			}

			result.set(sum);
			context.write(key, result);
		}

	}

	////// mapper 2
	////// output: <room, 0/1>
	public static class Mapper2 extends Mapper<Object,Text,Text, IntWritable>{
		private Text mykeyy =new Text();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			try{
			String line = value.toString();
			//System.out.println(line);
			String[] tokens=line.split("\\t");
			
			String a=tokens[0].toString();
		//	String b=tokens[1].toString();
		//	String c=tokens[2].toString();
			String d=tokens[3].toString();

			mykeyy.set(a);
			int no = Integer.parseInt(d);

			int zero = Integer.parseInt("0");
			int one = Integer.parseInt("1");


			IntWritable zero1 = new IntWritable(zero);
			IntWritable one1 = new IntWritable(one);
			//System.out.println(no);
			if(no>0)
			{
				//System.out.println("here1");
				context.write(mykeyy,one1);
			}
			else
			{
				//System.out.println("here0");
				context.write(mykeyy,zero1);
			}


			}catch(Exception e)
			{
			//System.out.println("exception2");
			}

		}

	}

	
	///// reducer 2
	///// output: <room, not in use percentage>
	public static class Reducer2
	extends Reducer<Text, IntWritable, Text, IntWritable>{
		private IntWritable result = new IntWritable();
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			int sum=0;

			for(IntWritable val:values){

				sum+=val.get();
			}

			result.set(((60-sum)*100)/60);
			context.write(key, result);

		}

	}

	public static void main(String[] args) throws Exception{
		Configuration conf =new Configuration();
		Job job=Job.getInstance(conf, "available");
		job.setJarByClass(EmptyRooms.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(myWritable.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
		System.out.println("Reducer1 output");

		Configuration conf2 =new Configuration();
		Job job2=Job.getInstance(conf2, "average");
		job2.setJarByClass(EmptyRooms.class);
		job2.setMapperClass(Mapper2.class);
		//job2.setCombinerClass(Reducer2.class);
		job2.setReducerClass(Reducer2.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job2, new Path(args[1]));
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));
		job2.waitForCompletion(true);
		System.exit(job2.waitForCompletion(true)?0:1);


	}
}

