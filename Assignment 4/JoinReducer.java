package Join;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class JoinReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> 
{
	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException 
	{ 
        List<String> myList = new ArrayList<String>() ;
        String str = "";
        
		while(values.hasNext()){
			str = values.next().toString();
         myList.add(str);
		}
        System.out.println(myList);
        int length = myList.size();
       
        for(int i=0;i<length;i++){
        	for(int j=i+1;j<length;j++){
        		String[] s1 = myList.get(i).split(",");
        		System.out.println(s1[0]);
        		String[] s2 = myList.get(j).split(",");
        		System.out.println(s2[0]);
        		if(!(s1[0].equals(s2[0])))
        				{
        				Text t = new Text(myList.get(j)+","+myList.get(i));
        				System.out.println(t);
        				output.collect(null, t);
        				}
        		
        	}
        	
        }
     /*   while (values.hasNext()) 
        { 
            Text value = values.next();  
            if(count == 0) 
            { 
                tag = new Text(value.getFirst()); 
                count = 1; 
            } 
             
            String line = value.getSecond().toString(); 
            String[] tuples = line.split(",");                 
            if(tuples[0].compareTo(tag.toString()) == 0) 
            { 
                T1.add(new Text(value.getSecond())); 
            } 
            else 
            { 
                
                for(Text val : T1) 
                {     
                    Text result = new Text(val.toString() + "," + value.getSecond().toString()); 
                    output.collect(key, result);
                } 
            } */                
        }
                   
    } 
 


