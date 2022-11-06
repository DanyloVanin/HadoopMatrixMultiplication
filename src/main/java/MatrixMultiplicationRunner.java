import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MatrixMultiplicationRunner{

    /**
     * MatrixMultiplicationRunner <input> <output>
     * @param args - cml args
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{

        Configuration conf = new Configuration();
        Job job = new Job(conf, "MatrixMultiply");
        
        // M = m*n matrix; N = n*p matrix 
        conf.set("m", "1000");
        conf.set("n", "100");
        conf.set("p", "1000");
        
        // Set Jar
        job.setJarByClass(MatrixMultiplicationRunner.class);
        
        // key/value types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        // Mapper/Reducer
        job.setMapperClass(MatrixMultiplicationMapper.class);
        job.setReducerClass(MatrixMultiplicationReducer.class);
        
        // input/output format
        job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class);
        job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.class);
        
        // input/output path
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
    
}
