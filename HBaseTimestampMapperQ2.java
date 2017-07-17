import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import com.cloudera.org.joda.time.DateTime;

public class HBaseTimestampMapperQ2 extends TableMapper<LongWritable, Text>
 { 
     public void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException 
     {  
    	     long articleID = Bytes.toLong(key.get(),0,8);
    	     long revisionID = Bytes.toLong(key.get(),8,8);
    	     String revisionIDStr = Long.toString(revisionID);
    	     long revisionTimestamp = value.rawCells()[0].getTimestamp(); 
    		 context.write(new LongWritable(articleID), new Text(revisionIDStr.concat(" " + Long.toString(revisionTimestamp))));
 	}
}