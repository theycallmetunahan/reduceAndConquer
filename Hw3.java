
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Hw3 {

    public static class SalaryMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable salary = new IntWritable();
        private final static Text word = new Text("Total");

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");

            if (fields.length > 1 && fields[3].trim().equals("Machine Learning Engineer")) {
                salary.set(Integer.parseInt(fields[6].trim()));
                context.write(word, salary);
            }
        }
    }

    public static class SalaryReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class JobTitleSalaryMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text jobTitle = new Text();
        private IntWritable salary = new IntWritable();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length > 1 && fields[0].equals("work_year") == false) {
                jobTitle.set(fields[3].trim());
                salary.set(Integer.parseInt(fields[6].trim()));
                context.write(jobTitle, salary);
            }
        }
    }

    public static class JobTitleSalaryReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
        private DoubleWritable averageSalary = new DoubleWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int count = 0;
            double average_current = 0;
            for (IntWritable val : values) {
                average_current = average_current * count + val.get();
                count++;
                average_current = average_current / count;
            }
            averageSalary.set(average_current);
            context.write(key, averageSalary);
        }
    }

    public static class JobTitleExperienceSalaryMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text jobTitleExperience = new Text();
        private IntWritable salary = new IntWritable();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length > 1 && !fields[0].equals("work_year")) {
                String experienceLevel = fields[1].trim();
                String jobTitle = fields[3].trim();
                if (experienceLevel.equals("SE") || experienceLevel.equals("MI") || experienceLevel.equals("EX")) {
                    jobTitleExperience.set(jobTitle + "_" + experienceLevel);
                    salary.set(Integer.parseInt(fields[6].trim()));
                    context.write(jobTitleExperience, salary);
                }
            }
        }
    }

    public static class JobTitleExperienceSalaryReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
        private DoubleWritable averageSalary = new DoubleWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int count = 0;
            double average_current = 0;
            for (IntWritable val : values) {
                average_current = average_current * count + val.get();
                count++;
                average_current = average_current / count;
            }
            averageSalary.set(average_current);
            context.write(key, averageSalary);
        }
    }

    public static class EmployeeResidenceMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text residenceCategory = new Text();
        private IntWritable salary = new IntWritable();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length > 1 && !fields[0].equals("work_year")) {
                String residence = fields[7].trim();
                salary.set(Integer.parseInt(fields[6].trim()));
                if (residence.equals("US")) {
                    residenceCategory.set("US");
                } else {
                    residenceCategory.set("NonUS");
                }
                context.write(residenceCategory, salary);
            }
        }
    }

    public static class ResidencePartitioner extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            if (key.toString().equals("US")) {
                return 0;
            } else {
                return 1;
            }
        }
    }

    public static class EmployeeResidenceReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
        private DoubleWritable averageSalary = new DoubleWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int count = 0;
            double average_current = 0;
            for (IntWritable val : values) {
                average_current = average_current * count + val.get();
                count++;
                average_current = average_current / count;
            }
            averageSalary.set(average_current);
            context.write(key, averageSalary);
        }
    }

    public static class WorkYearSalaryMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text yearCategory = new Text();
        private IntWritable salary = new IntWritable();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length > 1 && !fields[0].equals("work_year")) {
                String workYear = fields[0].trim();
                salary.set(Integer.parseInt(fields[6].trim()));
                if (workYear.equals("2024")) {
                    yearCategory.set("2024");
                } else if (workYear.equals("2023")) {
                    yearCategory.set("2023");
                } else {
                    yearCategory.set("Bef-2023");
                }
                context.write(yearCategory, salary);
            }
        }
    }

    public static class WorkYearPartitioner extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            if (key.toString().equals("2024")) {
                return 0;
            } else if (key.toString().equals("2023")) {
                return 1;
            } else {
                return 2;
            }
        }
    }

    public static class WorkYearSalaryReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
        private DoubleWritable averageSalary = new DoubleWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int count = 0;
            double average_current = 0;
            for (IntWritable val : values) {
                average_current = average_current * count + val.get();
                count++;
                average_current = average_current / count;
            }
            averageSalary.set(average_current);
            context.write(key, averageSalary);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Hw3");
        job.setJarByClass(Hw3.class);
        job.setJar("Hw3.jar");

        if (args[0].equals("total")) {
            job.setMapperClass(SalaryMapper.class);
            job.setCombinerClass(SalaryReducer.class);
            job.setReducerClass(SalaryReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
        } else if (args[0].equals("jobtitle")) {
            job.setMapperClass(JobTitleSalaryMapper.class);
            job.setReducerClass(JobTitleSalaryReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setOutputValueClass(DoubleWritable.class);
        } else if (args[0].equals("titleexperince")) {
            job.setMapperClass(JobTitleExperienceSalaryMapper.class);
            job.setReducerClass(JobTitleExperienceSalaryReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setOutputValueClass(DoubleWritable.class);
        } else if (args[0].equals("employeeresidence")) {
            job.setMapperClass(EmployeeResidenceMapper.class);
            job.setPartitionerClass(ResidencePartitioner.class);
            job.setReducerClass(EmployeeResidenceReducer.class);
            job.setNumReduceTasks(2);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setOutputValueClass(DoubleWritable.class);
        } else if (args[0].equals("averageyear")) {
            job.setMapperClass(WorkYearSalaryMapper.class);
            job.setPartitionerClass(WorkYearPartitioner.class);
            job.setReducerClass(WorkYearSalaryReducer.class);
            job.setNumReduceTasks(3);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setOutputValueClass(DoubleWritable.class);
        } else {
            System.out.println("Invalid argument");
            System.exit(1);
        }

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}