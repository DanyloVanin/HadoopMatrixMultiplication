import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MatrixMultiplicationMapper extends Mapper<LongWritable,Text,Text,Text>{

    @Override
    public void map(LongWritable inKey, Text inValue, Context context) throws IOException, InterruptedException{
        // Getting configuration parameters
        Configuration conf = context.getConfiguration();
        int m = Integer.parseInt(conf.get("m"));
        int p = Integer.parseInt(conf.get("p"));
        // Getting incoming value string. Expected format: (M|N),<row>,<col>,<value_row_col>
        String matrixLine = inValue.toString();
        String[] matrixElement = matrixLine.split(",");
        if (matrixElement[0].equals("M")){
            writeMatrixM(p, matrixElement, context);
        }
    }

    private void writeMatrixM(int p, String[] matrixElement, Context context) throws IOException, InterruptedException{
        // Input: MatrixElement = (M, i, j, M_ij)
        Text outputKey = new Text();
        Text outputValue = new Text();
        for (int k = 0; k < p; k++){
            outputKey.set(String.format("%s,%s", matrixElement[1], k));
            outputValue.set(String.format("M,%s,%s", matrixElement[2], matrixElement[3]));
            context.write(outputKey, outputValue);
        }
        // Output: (i,k) -> (M,j,M_ij)
    }

    private void writeMatrixN(int m, String[] matrixElement, Context context) throws IOException, InterruptedException{
        // Input: MatrixElement =  (N, j, k, N_jk)
        Text outputKey = new Text();
        Text outputValue = new Text();
        for (int i = 0; i < m; i++){
            outputKey.set(String.format("%s,%s", i, matrixElement[2]));
            outputValue.set(String.format("N,%s,%s", matrixElement[1], matrixElement[3]));
            context.write(outputKey, outputValue);
        }
        // Output: (i,k) -> (N,j,N_ij)
    }
}
