import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MutualFriends2 {

//----------------------Mapper--------Emits:- Key:No. of mutual friends, Value:friendpair+Mutualfriendslist----------

    public static class MutualFriends2_Mapper extends Mapper<LongWritable, Text, IntWritable, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String data[]=value.toString().split("\t");
            String frnd_pair=data[0];
            String frnds=data[1];
            String frnd_list[]=frnds.split(",");
            int count=frnd_list.length;

            IntWritable ke = new IntWritable();
            Text val = new Text();

            ke.set(count);
            val.set(frnd_pair+"_"+frnds);

            context.write(ke,val);

        }
    }
//-------------------Compare sorts the data in descending order based on the count value-----------------------
    public static class Compare extends WritableComparator {
        public Compare(){
            super(IntWritable.class, true);
        }
        @Override
        public int compare(WritableComparable wc1, WritableComparable wc2) {
            IntWritable k1 = (IntWritable) wc1;
            IntWritable k2 = (IntWritable) wc2;

            int res=k1.compareTo(k2)*(-1);
            return res;
        }
    }

//----------------Reducer outputs the first 10 entries in the file in the desired format----------------------
    public static class MutualFriends2_Reducer extends Reducer<IntWritable, Text, Text , Text> {

        int count=0;
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for(Text t:values){
                if(count==10){
                    break;
                }
                else{
                    count++;
                    String tuple[]=t.toString().split("_");
                    context.write(new Text(tuple[0]+"\t"+key.toString()),new Text(tuple[1]));
                }
            }

        }
    }

    public static void main(String args[]) throws Exception
    {
        Configuration conf=new Configuration();
        Job job = Job.getInstance(conf, "MF_2");

        job.setJarByClass(MutualFriends2.class);
        job.setSortComparatorClass(Compare.class);
        job.setMapperClass(MutualFriends2_Mapper.class);
        job.setReducerClass(MutualFriends2_Reducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));//----Mutual Friends file path
        FileOutputFormat.setOutputPath(job, new Path(args[1]));//---Output file path

        job.waitForCompletion(true);

    }
}

