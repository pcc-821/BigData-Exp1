import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GrandParent {
    public static class GPMap extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty() || line.startsWith("child")) return;
            String[] parts = line.split("\\s+");
            if (parts.length < 2) return;
            String child = parts[0];
            String parent = parts[1];
            context.write(new Text(parent), new Text(child + "\tL"));
            context.write(new Text(child), new Text(parent + "\tR"));
        }
    }

    public static class GPReduce extends Reducer<Text, Text, Text, Text> {
        private final Text grandchild = new Text();
        private final Text grandparent = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> children = new ArrayList<String>();
            List<String> parents = new ArrayList<String>();

            for (Text val : values) {
                String[] arr = val.toString().split("\t");
                if (arr[1].equals("L")) children.add(arr[0]);
                else parents.add(arr[0]);
            }

            for (String child : children) {
                for (String parent : parents) {
                    grandchild.set(child);
                    grandparent.set(parent);
                    context.write(grandchild, grandparent);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "grand_parent");
        job.setJarByClass(GrandParent.class);
        job.setMapperClass(GPMap.class);
        job.setReducerClass(GPReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
