import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;

public class MatrixMultiplicationReducer extends Reducer<Text,Text,Text,Text>{

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
        // Key = (i,k)
        // Value = ((M/N),j,value)
        HashMap<Integer,Double> hashM = new HashMap<>();
        HashMap<Integer,Double> hashN = new HashMap<>();

        // Remembering values for M and N matrices
        String[] valueElements;
        for (Text value : values){
            valueElements = value.toString().split(",");
            if (valueElements[0].equals("M")){
                hashM.put(Integer.parseInt(valueElements[1]), Double.parseDouble(valueElements[2]));
            } else{
                hashN.put(Integer.parseInt(valueElements[1]), Double.parseDouble(valueElements[2]));
            }
        }

        // Count sum
        int n = Integer.parseInt(context.getConfiguration().get("n"));
        double result = 0.0;
        double m_ij, n_jk;
        for (int j = 0; j < n; j++){
            m_ij = hashM.getOrDefault(j, 0.0);
            n_jk = hashN.getOrDefault(j, 0.0);
            result += m_ij * n_jk;
        }

        // Write result for (i,k)
        context.write(null, new Text(String.format("%s,%f", key.toString(), result)));
    }
}
