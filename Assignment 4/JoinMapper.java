package Join;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class JoinMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> 
{ 	
	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException 
	{ 
		String line = value.toString().trim(); 
        String[] tuples = line.split(",");
        
        Text keyName = new Text();
        keyName.set(tuples[1]);
        Text valueString = new Text();
        valueString.set(line);
        
       // Text tag = new Text(tuples[0]); 
        //Text tupleKey = new Text(tuples[1]);             
        //TextPair valueTag = new TextPair(tag,value); 
        output.collect(keyName, valueString); 
	} 	
	
} 
