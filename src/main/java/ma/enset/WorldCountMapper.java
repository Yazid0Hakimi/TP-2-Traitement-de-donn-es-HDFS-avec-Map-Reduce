package ma.enset;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WorldCountMapper extends Mapper<DoubleWritable, Text, Text, DoubleWritable> {
    @Override
    protected void map(DoubleWritable key, Text value, Mapper<DoubleWritable, Text, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
        // Split the input line by whitespace
        String[] tokens = value.toString().split(" ");

        // Check if the line contains the expected number of tokens (date, city, product, price)
        if (tokens.length == 4) {
            String city = tokens[1];  // Extract city from the input line
            double price = Double.parseDouble(tokens[3]);  // Extract price from the input line

            // Emit key-value pair with city as key and price as value
            context.write(new Text(city), new DoubleWritable(price));
        }
    }
}
