package jm;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxTemp {

	public static void main(String[] args) throws Exception {
		//맵 리듀스 작업 객체 정의
		Job job = new Job();		
		job.setJarByClass(MaxTemp.class);
		//JobConf job = new JobConf(MaxTemp.class); 
		
		job.setJobName("Max Temp");
		
		//맵리듀스 작업에 필요한 입출력 파일 지정
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//맵리듀스 작업시 사용할 mapper/reducer 지정
		job.setMapperClass(MaxTempMapper.class);
		job.setReducerClass(MaxTempReducer.class);
		
		//맵리듀스작업시 키/값 유형 정의
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//작업종료
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}

}

