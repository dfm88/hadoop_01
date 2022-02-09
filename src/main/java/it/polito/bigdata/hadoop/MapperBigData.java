package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Basic MapReduce Project - Mapper
 */
class MapperBigData extends Mapper<LongWritable, // Input key type
        Text,         // Input value type
        Text,         // Output key type
        IntWritable> {// Output value type

    protected void map(LongWritable key,   // Input key type
                       Text value,         // Input value type
                       Context context) throws IOException, InterruptedException {

        // Split each sentence in words. Use whitespace(s) as delimiter
        // (=a space, a tab, a line break, or a form feed)
        // The split method returns an array of strings
        String[] words = value.toString().split("\\s+");

//            // Iterate over the set of words
//            for(String word : words) {
//            	// Transform word case
//                String cleanedWord = word.toLowerCase();
//
//                // emit the pair (word, 1)
//                context.write(new Text(cleanedWord),
//                		      new IntWritable(1));
//            }
        // 2-grams version
        // Get words from the input line

        for (int i=0; i<words.length-1; i++) {
            String biGram = words[i] + " " + words[i+1];
            biGram = biGram.toLowerCase();

            /**
             * Cleaning (optional for this lab exercise)
             * matches the bigram only if composed by alphanumeric characters
             */
            if (biGram.matches("[a-z0-9]+ [a-z0-9]+"))
                context.write(new Text(biGram), new IntWritable(1));
        }
    }
}
