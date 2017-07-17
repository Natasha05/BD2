import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class HBaseTimestampPartitionerQ1 extends Partitioner<LongWritable, Text> 
 {
      public int getPartition(LongWritable key, Text value, int numReducers) 
      { 
    	  Long partitionKey = Long.parseLong((key.toString()));
          return (int) ((partitionKey * numReducers)/16000000);
      }
 }     
