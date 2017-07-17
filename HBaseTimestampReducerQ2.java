import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.cloudera.org.joda.time.DateTime;

public class HBaseTimestampReducerQ2 extends Reducer<LongWritable, Text, Long, Text> {
	HashMap<Long, Text> HashMapReducer = new HashMap<Long, Text>();
	List<Entry<Long, Text>> list = null;

	public void reduce(LongWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		String mapperStr = "", revisionID = "", revTimeStamp = "";
		for (Text val : values) 
		{
			mapperStr = val.toString();
			revisionID = mapperStr.substring(0, mapperStr.indexOf(" "));
			revTimeStamp = mapperStr.substring(mapperStr.indexOf(" ") + 1);
			
			if(HashMapReducer.containsKey(key.get()))
			{
				String parseString = HashMapReducer.get(key.get()).toString();
				String parseRevisionID = parseString.substring(0,parseString.indexOf(" "));
				String parseTimestamp = parseString.substring(parseString.indexOf(" ") + 1);
				
				if(Long.parseLong(revTimeStamp) > Long.parseLong(parseTimestamp))
				{
					HashMapReducer.put(Long.parseLong(key.toString()), new Text(revisionID + " " + revTimeStamp));
				}

				
			} 
			else 
			{
				HashMapReducer.put(Long.parseLong(key.toString()), new Text(revisionID + " " + revTimeStamp));
			}
			
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		list = new ArrayList<Entry<Long, Text>>(HashMapReducer.entrySet());
		Collections.sort(list, new Comparator<HashMap.Entry<Long, Text>>() {
			public int compare(HashMap.Entry<Long, Text> obj1, HashMap.Entry<Long, Text> obj2) {
				if (obj1.getKey() > obj2.getKey())
					return 1;
				if (obj1.getKey() < obj2.getKey())
					return -1;
				return 0;
			}
		});

		for (HashMap.Entry<Long, Text> entry : list) 
		{
			String mapperString = entry.getValue().toString();
			String revisionID = mapperString.substring(0, mapperString.indexOf(" "));
			String revTimeStamp = mapperString.substring(mapperString.indexOf(" ") + 1);
			Date revisionTimestamp = new Date(Long.parseLong(revTimeStamp));
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
			String timeStampStr = sdf.format(revisionTimestamp);
			context.write(entry.getKey(), new Text(revisionID + " " + timeStampStr));
		}

	}
}