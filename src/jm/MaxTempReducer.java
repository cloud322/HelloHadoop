package jm;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxTempReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		//최고 온도를 0 으로설정
		int maxValue =Integer .MIN_VALUE;
		
		//맵의 결과로 추출된 년도와 온도정보가 들어 있는 values 변수를 for 문으로 순환하여
		//최고온도를 조사
		for(IntWritable value : values)
		{
			maxValue = Math.max(maxValue, value.get());
		}
		//최고온도를 결과로 출력
		context.write(key, new IntWritable(maxValue));
	}

}
