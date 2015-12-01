package Join;

import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.*; 
import org.apache.hadoop.mapred.*; 
public class equijoin 
{ 
	public static void main(String[] args) throws Exception 
    { 
        JobConf config = new JobConf(equijoin.class); 
        
        String inputPath  = args[0]; 
        String outputPath = args[1]; 
        
        config.setJobName("EquiJoin"); 	         
        config.setOutputKeyClass(Text.class); 
        config.setOutputValueClass(Text.class); 	         
        config.setMapperClass(JoinMapper.class); 
        config.setReducerClass(JoinReducer.class); 	 
        config.setInputFormat(TextInputFormat.class); 
        config.setOutputFormat(TextOutputFormat.class); 
        	        
       // EquiJoin join = new EquiJoin(); 	         
        FileInputFormat.setInputPaths(config, new Path(inputPath)); 
        FileOutputFormat.setOutputPath(config, new Path(outputPath)); 	 
        JobClient.runJob(config); 
    } 
	
}
