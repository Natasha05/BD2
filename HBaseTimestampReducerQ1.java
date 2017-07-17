import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Scanner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HBaseTimestampReducerQ1 extends Reducer<LongWritable, Text, Long, Text> 
{
	HashMap<Long, Text> HashMapReducer = new HashMap<Long, Text>();
	List<Entry<Long, Text>> list = null;
	
	public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	{
	    int numOfRevision = 0;
	    
	    String mapperStr = "", revisionCount, revisionID = "";
	    StringBuilder sbRevisionIds = new StringBuilder();

	    for (Text val : values) 
	    {
	    	mapperStr = val.toString();
	    	revisionCount = mapperStr.substring(0, mapperStr.indexOf(" "));
	    	revisionID = mapperStr.substring(mapperStr.indexOf(" ")).concat(revisionID + " ");	       
	       numOfRevision += Integer.parseInt(revisionCount);
	    }
	    
	    Long[] revisionIdArray = new Long[numOfRevision];
	    
	    Scanner scan = new Scanner(revisionID);
	    
	    for (int revisionIdArrayIndex=0; revisionIdArrayIndex < revisionIdArray.length; revisionIdArrayIndex++)
	    {
	    	revisionIdArray[revisionIdArrayIndex] = Long.parseLong(scan.next());
	    }
	    
	    Arrays.sort(revisionIdArray);
	   
	    for(int revisionIdStringIndex =0; revisionIdStringIndex < revisionIdArray.length; revisionIdStringIndex++)
	    {
	    	sbRevisionIds.append(revisionIdArray[revisionIdStringIndex].toString());
	    	sbRevisionIds.append(" ");
	    }
	    HashMapReducer.put(Long.parseLong(key.toString()), new Text(Integer.toString(numOfRevision).concat(" " + sbRevisionIds.toString())));
	    
	  }
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException 
	{
		list = new ArrayList<Entry<Long, Text>>(HashMapReducer.entrySet());
		Collections.sort(list, new Comparator<HashMap.Entry<Long, Text>>() 
		{
			public int compare(HashMap.Entry<Long, Text> obj1, HashMap.Entry<Long, Text> obj2) 
			{
				if (obj1.getKey() > obj2.getKey())
					return 1;
				if (obj1.getKey() < obj2.getKey())
					return -1;
				return 0;
			}
		});
		
		for (HashMap.Entry<Long, Text> entry : list) 
		{
		    context.write(entry.getKey(), entry.getValue());
		} 
		
		
	}
}