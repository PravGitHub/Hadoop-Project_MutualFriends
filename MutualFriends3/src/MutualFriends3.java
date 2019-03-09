import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;


public class MutualFriends3 extends Configured {
    static HashMap<String, String> userData;

//--------------------------Mapper performs mapside join and emits the mutual friend details--------------

    public static class MutualFriends3_Mapper extends Mapper<LongWritable, Text, Text, Text>{

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
                    String value = splitLine[1] + ":" + splitLine[4];
                    userData.put(splitLine[0], value);
                    line = br.readLine();
                }
            }

        }
//----------------------Map-----Emits:--key:friendpair, value: data of 1 mutual friend--------------

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int A = Integer.parseInt(conf.get("A"));
            int B = Integer.parseInt(conf.get("B"));
            int c;
            if(B<A){//------Swapping values to make (A,B)=(B,A)
                c=A;
                A=B;
                B=c;
            }

            String linesplit[]=value.toString().split("\t");
            String ke[]=linesplit[0].split(",");
            int A1=Integer.parseInt(ke[0]);
            int B1=Integer.parseInt(ke[1]);

            if(A==A1 && B==B1){
                String mfs[]=linesplit[1].split(",");

                if (userData!=null) {
                    for (String mf : mfs) {
                        if (userData.containsKey(mf)) {
                            String data = userData.get(mf);
                            context.write(new Text(linesplit[0]), new Text(data));
                        }
                    }
                }
            }

        }
    }
//--------------------Reducer outputs the friendpair and detals of every mutual friend----------------

    public static class MutualFriends3_Reducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
            ArrayList<String> details= new ArrayList<>();

            for (Text val:values) {
                details.add(val.toString());
            }

            context.write(key,new Text("["+StringUtils.join(",", details)+"]"));
        }
    }


    public static void main(String args[]) throws Exception {
        Configuration conf = new Configuration();

        conf.set("A", args[0].trim());
        conf.set("B", args[1].trim());
        conf.set("userDataPath",args[2]);

        Job job = Job.getInstance(conf, "MFs3");
        job.setJarByClass(MutualFriends3.class);
        job.setMapperClass(MutualFriends3_Mapper.class);
        job.setReducerClass(MutualFriends3_Reducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[3]));//--------mutual-friends file path
        FileOutputFormat.setOutputPath(job, new Path(args[4]));//------Output file path

        job.waitForCompletion(true);
    }

}