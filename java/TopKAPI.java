package org.myorg;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.StringTokenizer;
import java.util.concurrent.ThreadLocalRandom;



// Implementation of TopKAPI algorithm
// https://papers.nips.cc/paper/8287-topkapi-parallel-and-fast-sketches-for-finding-top-k-frequent-elements

public class TopKAPI {

    // Sketch is the core structure in topkapi algorithm, it's a reduce-able summary of frequent words.
    public static class Sketch implements WritableComparable <Sketch> {
        int num_hash;
        int num_counter;
        int[] hasha;
        int[] hashb;
        int[][] LHHCounter;
        int[][] CMSCounter;
        String[][] LHHString;

        Sketch(int _num_hash, int _num_counter, int[] _hasha, int[] _hashb) {
            num_hash = _num_hash;
            num_counter = _num_counter;
            LHHCounter = new int[num_hash][num_counter];
            CMSCounter = new int[num_hash][num_counter];
            LHHString = new String[num_hash][num_counter];
            hasha = new int[num_hash];
            hashb = new int[num_hash];
            for(int i = 0; i < num_hash; i++) {
                for(int j = 0; j < num_counter; j++) {
                    LHHString[i][j] = "null";
                }
            }
            for(int i = 0; i < num_hash; i++) {
                hasha[i] = _hasha[i];
                hashb[i] = _hashb[i];
            }
        }

        Sketch() { }

        /// hash a string to integer number
        long stringToInt(String s) {
            long h = 0;
            long mod = (long)1e9 + 7;
            for(char c : s.toCharArray()) {
                h = (h*255 + Character.getNumericValue(c) + 128) % mod;
            }
            return h;
        }

        /// implement WritableComparable
        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(num_hash);
            out.writeInt(num_counter);
            for(int i = 0; i < num_hash; i++) {
                out.writeInt(hasha[i]);
                out.writeInt(hashb[i]);
            }
            for(int i = 0; i < num_hash; i++) {
                for(int j = 0; j < num_counter; j++) {
                    out.writeInt(LHHCounter[i][j]);
                }
            }
            for(int i = 0; i < num_hash; i++) {
                for(int j = 0; j < num_counter; j++) {
                    out.writeInt(CMSCounter[i][j]);
                }
            }
            for(int i = 0; i < num_hash; i++) {
                for(int j = 0; j < num_counter; j++) {
                    out.writeUTF(LHHString[i][j]);
                }
            }
        }

        /// implement WritableComparable
        @Override
        public void readFields(DataInput in) throws IOException {
            num_hash = in.readInt();
            num_counter = in.readInt();
            hasha = new int[num_hash];
            hashb = new int[num_hash];
            LHHCounter = new int[num_hash][num_counter];
            CMSCounter = new int[num_hash][num_counter];
            LHHString = new String[num_hash][num_counter];
            for(int i = 0; i < num_hash; i++) {
                hasha[i] = in.readInt();
                hashb[i] = in.readInt();
            }
            for(int i = 0; i < num_hash; i++) {
                for(int j = 0; j < num_counter; j++) {
                    LHHCounter[i][j] = in.readInt();
                }
            }
            for(int i = 0; i < num_hash; i++) {
                for(int j = 0; j < num_counter; j++) {
                    CMSCounter[i][j] = in.readInt();
                }
            }
            for(int i = 0; i < num_hash; i++) {
                for(int j = 0; j < num_counter; j++) {
                    LHHString[i][j] = in.readUTF();
                }
            }
        }

        /// implement WritableComparable
        @Override
        public int compareTo(Sketch s) {
            return 0;
        }

        /// add a new word to this Sketch
        void addWord(String s) {
            long h = stringToInt(s);
            for(int i = 0; i < num_hash; i++) {
                int hh = (int)((h*(long)hasha[i] + (long)hashb[i]) %(long)(1e9 + 7) % (long)num_counter);
                CMSCounter[i][hh] += 1;
                if(LHHCounter[i][hh] == 0) {
                    LHHCounter[i][hh] = 1;
                    LHHString[i][hh] = s;
                }
                else if(LHHString[i][hh].equals(s)) {
                    LHHCounter[i][hh] += 1;
                }
                else {
                    LHHCounter[i][hh] -= 1;
                }
            }
        }

        /// struct to sort and output result
        class Word implements Comparable <Word> {
            String s;
            int count;
            Word(String s, int count) {
                this.s = s;
                this.count = count;
            }

            @Override
            public int compareTo(Word w) {
                return - this.count + w.count;
            }
        }

        class WordCmp implements Comparator<Word> {
            public int compare(Word a, Word b) {
                return b.count - a.count;
            }
        }

        /// write result in reducer
        void writeContext(Reducer.Context context) throws IOException, InterruptedException {
            ArrayList<Word> wl = new ArrayList<Word>();
            for(int i = 0; i < num_hash; i++) {
                for(int j = 0; j < num_counter; j++) {
                    boolean f = false;
                    for(Word w: wl) {
                        if(w.s.equals(LHHString[i][j])) {
                            f = true;
                            break;
                        }
                    }
                    if(!f) {
                        wl.add(new Word(LHHString[i][j], 0));
                    }
                }
            }

            for(Word w: wl) {
                long h = stringToInt(w.s);
                int cnt = (int)1e9;
                for(int i = 0; i < num_hash; i++) {
                    int hh = (int)((h*(long)hasha[i] + (long)hashb[i]) %(long)(1e9 + 7)% (long)num_counter);
                    if(CMSCounter[i][hh] < cnt) {
                        cnt = CMSCounter[i][hh];
                    }
                }
                w.count = cnt;
            }
            wl.sort(new WordCmp());

            Configuration conf = context.getConfiguration();
            int K = conf.getInt("K", -1);
            int output_cnt = 0;
            for(Word w: wl) {
                context.write(w.count, w.s);
                output_cnt += 1;
                if(output_cnt >= K) {
                    break;
                }
            }
        }
    }

    /// merge two sketch, it's the core of reduction
    private static Sketch mergeSketch(Sketch A, Sketch B) {
        Sketch C = new Sketch(A.num_hash, A.num_counter, A.hasha, A.hashb);
        for(int i = 0; i < A.num_hash; i++) {
            for(int j = 0; j < A.num_counter; j++) {
                C.CMSCounter[i][j] = A.CMSCounter[i][j] + B.CMSCounter[i][j];
            }
        }
        for(int i = 0; i < A.num_hash; i++) {
            for(int j = 0; j < A.num_counter; j++) {
                if(A.LHHString[i][j].equals(B.LHHString[i][j])) {
                    C.LHHString[i][j] = A.LHHString[i][j];
                    C.LHHCounter[i][j] = A.LHHCounter[i][j] + B.LHHCounter[i][j];
                }
                else {
                    if(A.LHHCounter[i][j] > B.LHHCounter[i][j]) {
                        C.LHHCounter[i][j] = A.LHHCounter[i][j] - B.LHHCounter[i][j];
                        C.LHHString[i][j] = A.LHHString[i][j];
                    }
                    else {
                        C.LHHCounter[i][j] = B.LHHCounter[i][j] - A.LHHCounter[i][j];
                        C.LHHString[i][j] = B.LHHString[i][j];
                    }
                }
            }
        }
        return C;
    }

    public static class Map extends Mapper<LongWritable, Text, IntWritable, Sketch> {
        private final static IntWritable one = new IntWritable(1);
        private Sketch sketch;

        // The parameter of hash functions must be setup before map function
        @Override
        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            int num_hash = conf.getInt("num_hash", -1);
            int num_counter = conf.getInt("num_counter", -1);
            int[] hasha = new int[num_hash];
            int[] hashb = new int[num_hash];
            for(int i = 0; i < num_hash; i++) {
                hasha[i] = conf.getInt("hasha_"+Integer.toString(i), -1);
                hashb[i] = conf.getInt("hashb_"+Integer.toString(i), -1);
            }
            sketch = new Sketch(num_hash, num_counter, hasha, hashb);
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            context.write(one, sketch);
        }

        // Input key: default, don't use
        // Input value: text to be counted
        // Output key: 1, since we don't need to sort sketch, output key can be anything.
        // Output value: a Sketch structure.

        // Input file format same to wordcount
        /*
          word1 word2 word3 word4
          word5 ...
         */

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                sketch.addWord(tokenizer.nextToken());
            }
        }
    }

    public static class Reduce extends Reducer<IntWritable, Sketch, IntWritable, Text> {
        Sketch summary;
        // The parameter of hash functions must be setup before map function
        @Override
        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            int num_hash = conf.getInt("num_hash", -1);
            int num_counter = conf.getInt("num_counter", -1);
            int[] hasha = new int[num_hash];
            int[] hashb = new int[num_hash];
            for(int i = 0; i < num_hash; i++) {
                hasha[i] = conf.getInt("hasha_"+Integer.toString(i), -1);
                hashb[i] = conf.getInt("hashb_"+Integer.toString(i), -1);
            }
            summary = new Sketch(num_hash, num_counter, hasha, hashb);
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            summary.writeContext(context);
        }

        // Input key: 1
        // Input value: Sketchs
        // Output key: approximate count of a word
        // Output value: word string

        // The format of output file is like:
        // count_of_word1 word1
        // count_of_word2 word2
        // ...
        // count sorted in decreasing order, so the approximate topk is just top k lines.

        /*
          25838902	0
          20286179	1
          16716986	2
          14175145	3
          12320167	4
          10889307	5
          9756750	6
          8836840	7
          8074894	8
          7446808	9
          6879691	10
          6407170	11
          5995123	12
          5649203	13
          5316165	14
          5053041	15
         */
        @Override
        public void reduce(IntWritable key, Iterable<Sketch> values, Context context)
                throws IOException, InterruptedException {
            for (Sketch val : values) summary = mergeSketch(summary, val);
        }
    }

    public static void main(String[] args) throws Exception {

        /// provide a simple CLI to control parameters
        Options options = new Options();
        CommandLineParser parser = new PosixParser();
        options.addOption(OptionBuilder.withLongOpt("CMS_l")
                .withDescription("number of uniform hash functions in CMS, default: 4")
                .hasArg()
                .withArgName("CMS_l")
                .create());

        options.addOption(OptionBuilder.withLongOpt("CMS_b")
                .withDescription("number of counters for each hash function in CMS table, default: 1024")
                .hasArg()
                .withArgName("CMS_b")
                .create());

        options.addOption(OptionBuilder.withLongOpt("K")
                .withDescription("limit of number of words (at most CMS_l*CMS_b) to output, default: inf")
                .hasArg()
                .withArgName("K")
                .create());

        options.addOption(OptionBuilder.withLongOpt("input")
                .withDescription("input file path, default: /input")
                .hasArg()
                .withArgName("input_path")
                .create());

        options.addOption(OptionBuilder.withLongOpt("output")
                .withDescription("output file path, default: /output")
                .hasArg()
                .withArgName("output_path")
                .create());

        options.addOption(new Option("help", "print help message"));

        int CMS_l = 4;
        int CMS_b = 1024;
        int K = 1000000000;
        String input_path = "/input";
        String output_path = "/output";
        try {
            CommandLine line = parser.parse(options, args);
            if(line.hasOption("help")) {
                String header = "\n=======================================\n" +
                        "This is a hadoop implementation of TopKAPI algorithm \n" +
                        "https://papers.nips.cc/paper/8287-topkapi-parallel-and-fast-sketches-for-finding-top-k-frequent-elements.pdf\n" +
                        "Provide -K option to limit number of output\n" +
                        "CMS_l and CMS_b controls the precision of approximation, higher value leads to more accurate result, but more computationally expensive\n" +
                        "=======================================\n\n";
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("hadoop jar topkapi.jar [options]", header, options, "");
                System.exit(0);
            }
            input_path = line.getOptionValue("input", "/input");
            output_path = line.getOptionValue("output", "/output");
            K = Integer.parseInt(line.getOptionValue("K", "1000000000"));
            CMS_l = Integer.parseInt(line.getOptionValue("CMS_l", "4"));
            CMS_b = Integer.parseInt(line.getOptionValue("CMS_b", "1024"));
        }
        catch(ParseException exp ) {
            System.err.println( "CLI Parsing failed.  Reason: " + exp.getMessage());
            System.exit(1);
        }

        /// generate parameters of uniform independent hashing functions, and pass them to global cache
        Configuration conf = new Configuration();
        conf.setInt("num_hash", CMS_l);
        conf.setInt("num_counter", CMS_b);
        for(int i = 0; i < CMS_l; i++) {
            conf.setInt("hasha_"+Integer.toString(i), ThreadLocalRandom.current().nextInt(1, (int)1e9 + 7));
            conf.setInt("hashb_"+Integer.toString(i), ThreadLocalRandom.current().nextInt(0, (int)1e9 + 7));
        }


        /// job driver
        Job job = new Job(conf, "AHH"); // AHH: Approximate Heavy Hitter

        job.setJarByClass(org.myorg.TopKAPI.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Sketch.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(input_path));
        FileOutputFormat.setOutputPath(job, new Path(output_path));

        job.waitForCompletion(true);
    }
}
