import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Calendar;
import java.util.HashMap;

public class MutualFriends4 extends Configured {

//---Job1--------------------------mapper1 emits the direct friends------------------------------------------------

    public static class MutualFriends4_Mapper1 extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line[]=value.toString().split("\t");
            if(line.length==2) {
                context.write(new Text(line[0]), new Text(line[1]));
            }

        }
    }

//---Job1------------------------reducer1 does reduce side join---Emits:- key:personID, value:details+average age---

    public static class MutualFriends4_Reducer1 extends Reducer<Text,Text,Text,Text>{
        static HashMap<String, String> userData;
        int year;

        public void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            String line=null;
            userData = new HashMap<>();
            String dataPath=conf.get("userDataPath");

            Path path = new Path("hdfs://localhost:9000"+dataPath);
            FileSystem fs = FileSystem.get(conf);
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));

            line=br.readLine();

            while(line!=null){
                String splitLine[]=line.split(",");
                if(splitLine.length==10) {
                    String value = splitLine[3] + "," + splitLine[4]+","+splitLine[5]+"_"+splitLine[9];
                    userData.put(splitLine[0].trim(), value);
                    line = br.readLine();
                }
            }

            Calendar cal = Calendar.getInstance();
            year = cal.get(Calendar.YEAR);
        }

        public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException
        {
            String ke=key.toString();
            for(Text t:values) {
                String vals[] = t.toString().split(",");

                int vals_length = vals.length;
                int yr_sum = 0;

                for (String val : vals) {
                    if (userData.containsKey(val)) {

                        String[] record = userData.get(val).split("/");
                        if (record.length == 3) {
                            int yr = Integer.parseInt(record[2]);
                            yr_sum += (year - yr);
                        }
                    }
                }

                float avg = (float) yr_sum / (float) vals_length;
                String emit_val = userData.get(ke) + "#" + avg;

                context.write(key, new Text(emit_val));
            }
        }
    }

//-----Job2----------------mapper2-----Emits:- Key:averageAge, value:details----------------------------------

    public static class MutualFriends4_Mapper2 extends Mapper<LongWritable, Text, FloatWritable, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] rec=value.toString().split("#");
            float avg=0;
            FloatWritable fw=new FloatWritable();
            if(rec.length==2){
                avg=Float.parseFloat(rec[1]);
            }
            fw.set(avg);
            context.write(fw,new Text(rec[0]));

        }

    }
//-------Job2------------Compare sorts the file in descending order based on the avgAge-------------------
    public static class Compare extends WritableComparator {
        public Compare(){
            super(FloatWritable.class, true);
        }
        @Override
        public int compare(WritableComparable wc1, WritableComparable wc2) {
            FloatWritable k1 = (FloatWritable) wc1;
            FloatWritable k2 = (FloatWritable) wc2;

            int res=k1.compareTo(k2)*(-1);
            return res;
        }
    }
//------Job2------------Reducer2-----------Writes----Key:personID, value: details+avgAge-------------------
    public static class MutualFriends4_Reducer2 extends Reducer<FloatWritable,Text,Text,Text> {

        public void reduce(FloatWritable key,Iterable<Text> values,Context context) throws IOException, InterruptedException
        {
            String avg=key.toString();
            for(Text val:values){
                String[] split=val.toString().split("\t");
                String person=split[0];
                String[] deets=split[1].split("_");
                context.write(new Text(person),new Text(deets[0]+","+avg));
            }
        }
    }

//---------------------Main-------------Executes 2 Jobs-----------------------------------------------
    public static void main(String args[]) throws Exception
    {
        Configuration conf=new Configuration();
        conf.set("userDataPath",args[0]);
        Job job = Job.getInstance(conf, "MF_4_Avg");

        job.setJarByClass(MutualFriends4.class);

        job.setMapperClass(MutualFriends4_Mapper1.class);
        job.setReducerClass(MutualFriends4_Reducer1.class);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));//--------Friend list file
        FileOutputFormat.setOutputPath(job, new Path(args[2]));//------intermediate file

        job.waitForCompletion(true);

        Configuration conf1=new Configuration();
        Job job1 = Job.getInstance(conf1, "MF_4_Sort");

        job1.setJarByClass(MutualFriends4.class);

        job1.setSortComparatorClass(Compare.class);
        job1.setMapperClass(MutualFriends4_Mapper2.class);
        job1.setReducerClass(MutualFriends4_Reducer2.class);

        job1.setMapOutputKeyClass(FloatWritable.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1, new Path(args[2]));//--------intermediate file
        FileOutputFormat.setOutputPath(job1, new Path(args[3]));//------Output File

        job1.waitForCompletion(true);

    }

}
