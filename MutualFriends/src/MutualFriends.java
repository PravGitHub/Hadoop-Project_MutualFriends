import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;

import java.util.StringTokenizer;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;


public class MutualFriends
{
//-------------mapper--------Emits:- Key:friend_pair, value:friend list--------------------

    public static class MutualFriends_Mapper extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            String data[]=value.toString().split("\t");
            String person=data[0];
            int pno=Integer.parseInt(person);
            int frndno;
            String keytosend;

            if(data.length==2) {
                String frnd_list=data[1];
                StringTokenizer frnds = new StringTokenizer(data[1], ",");

                while(frnds.hasMoreTokens()){
                    frndno=Integer.parseInt(frnds.nextToken());

                    if(pno<frndno){
                        keytosend=pno+","+frndno;
                        context.write(new Text(keytosend),new Text(frnd_list));
                    }
                    else
                    {
                        keytosend=frndno+","+pno;
                        context.write(new Text(keytosend),new Text(frnd_list));
                    }
                }

            }

        }

    }
//------------------Reducer finds mutual friends and outputs the result-----------------------------

    public static class MutualFriends_Reducer extends Reducer<Text, Text, Text , Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String frnd_groups[]=new String[2];
            int i=0;
            String group1[];
            String group2[];
            for(Text t:values)
            {
                frnd_groups[i]=t.toString();
                i++;
            }

            ArrayList<String> g1=new ArrayList<>();
            String fin=null;

            if(frnd_groups[0]!=null){
                group1=frnd_groups[0].split(",");
                for(String g:group1){
                    g1.add(g);
                }
            }
            if(frnd_groups[1]!=null){
                group2=frnd_groups[1].split(",");
                boolean j=TRUE;
                for(String g:group2){
                    if(g1.contains(g)){
                        if(j==TRUE) {
                            fin=g;
                            j=FALSE;
                        }
                        else{
                            fin=fin+","+g;
                        }
                    }
                }
            }
            if(fin!=null) {
                context.write(key, new Text(fin));
            }
        }

    }


    public static void main(String args[]) throws Exception
    {
        Configuration conf=new Configuration();
        Job job = Job.getInstance(conf, "MF");

        job.setJarByClass(MutualFriends.class);
        job.setMapperClass(MutualFriends_Mapper.class);
        job.setReducerClass(MutualFriends_Reducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));//----Direct friends file path
        FileOutputFormat.setOutputPath(job, new Path(args[1]));//-----Output file path

        job.waitForCompletion(true);

    }
}
