import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class HBaseTimestampMapperQ1 extends TableMapper<LongWritable, Text>
 { 
	 private static final IntWritable one = new IntWritable(1);
     public void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException 
     {
    	          
    	     long articleID = Bytes.toLong(key.get(),0,8);
    	     long revisionID = Bytes.toLong(key.get(),8,8);
    	     String revisionIDStr = Long.toString(revisionID);
    		 context.write(new LongWritable(articleID), new Text(one.toString().concat(" " + revisionIDStr)));
 	}
}