import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.cloudera.org.joda.time.DateTime;

public class HBaseTimestampDriverQ2 extends Configured implements Tool 
 { 
    public int run(String[] args) throws Exception
    {
		Configuration conf = HBaseConfiguration.create(getConf());
		Job job = new Job(conf);
		job.setJarByClass(HBaseTimestampDriverQ2.class);
		final Scan scan = new Scan().setFilter(new FilterList(
                                     new FirstKeyOnlyFilter(),
                                     new KeyOnlyFilter()));
		scan.setTimeRange(0, new DateTime(args[0]).getMillis());
		conf.set("argumentTimestamp", args[0]);
		scan.addColumn(Bytes.toBytes("WD"), Bytes.toBytes("TITLE"));
		scan.setCaching(5000);
		scan.setCacheBlocks(false);
		TableMapReduceUtil.initTableMapperJob(args[1], scan, HBaseTimestampMapperQ2.class, LongWritable.class, Text.class,job);
		job.setReducerClass(HBaseTimestampReducerQ2.class);
		job.setPartitionerClass(HBaseTimestampPartitionerQ2.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new HBaseTimestampDriverQ2(), args));
	}
}
