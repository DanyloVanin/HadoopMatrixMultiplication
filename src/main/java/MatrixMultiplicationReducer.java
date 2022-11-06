import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;

public class MatrixMultiplicationReducer extends Reducer<Text,Text,Text,Text>{

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
        // Key = (i,k)
        // Value = ((M/N),j,value)
        HashMap<Integer,Float> hashM = new HashMap<>();
        HashMap<Integer,Float> hashN = new HashMap<>();

        // Remembering values for M and N matrices
        String[] valueElements;
        for (Text value : values){
            valueElements = value.toString().split(",");
            if (valueElements[0].equals("M")){
                hashM.put(Integer.parseInt(valueElements[1]), Float.parseFloat(valueElements[2]));
            } else{
                hashN.put(Integer.parseInt(valueElements[1]), Float.parseFloat(valueElements[2]));
            }
        }

        // Count sum
        int n = Integer.parseInt(context.getConfiguration().get("n"));
        float result = 0.0f;
        float m_ij, n_jk;
        for (int j = 0; j < n; j++){
            m_ij = hashM.getOrDefault(j, 0.0f);
            n_jk = hashN.getOrDefault(j, 0.0f);
            result += m_ij * n_jk;
        }

        // Write result for (i,k)
        if (result != 0.0f){
            context.write(null, new Text(String.format("%s,%f", key.toString(), result)));
        }
    }
}
