package com.zhangsr;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.net.URI;
import java.util.List;
import java.util.Random;
import java.io.InputStreamReader;

public class myKMeans {
    private int k;
    private int iterationNum;
    private String sourcePath;
    private String outputPath;

    private Configuration conf;

    public myKMeans(int k, int iterationNum, String sourcePath, String outputPath, Configuration conf){
        this.k = k;
        this.iterationNum = iterationNum;
        this.sourcePath = sourcePath;
        this.outputPath = outputPath;
        this.conf = conf;
    }

    public void clusterCenterJob() throws IOException, InterruptedException, ClassNotFoundException{
        for(int i = 0;i < iterationNum; i++){
            Job clusterCenterJob = Job.getInstance(conf, "clusterCenterJob"+i);
            clusterCenterJob .setJarByClass(KMeansAlgorithm.class);

            clusterCenterJob.getConfiguration().set("clusterPath", outputPath + "/cluster-" + i +"/");

            clusterCenterJob.setMapperClass(KMeansAlgorithm.KMeansMapper.class);
            clusterCenterJob.setMapOutputKeyClass(IntWritable.class);
            clusterCenterJob.setMapOutputValueClass(Cluster.class);

            clusterCenterJob.setCombinerClass(KMeansAlgorithm.KMeansCombiner.class);
            clusterCenterJob.setReducerClass(KMeansAlgorithm.KMeansReducer .class);
            clusterCenterJob.setOutputKeyClass(NullWritable.class);
            clusterCenterJob.setOutputValueClass(Cluster.class);

            FileInputFormat.addInputPath(clusterCenterJob, new Path(sourcePath));
            FileOutputFormat.setOutputPath(clusterCenterJob, new Path(outputPath + "/cluster-" + (i + 1) +"/"));

            clusterCenterJob.waitForCompletion(true);
            System.out.println("finished!");
        }
    }

    public void KMeansClusterJob() throws IOException, InterruptedException, ClassNotFoundException{
        Job kMeansClusterJob = Job.getInstance(conf, "KMeansClusterJob");
        kMeansClusterJob.setJarByClass(KMeansCluster.class);

        kMeansClusterJob.getConfiguration().set("clusterPath", outputPath + "/cluster-" + (iterationNum - 1) +"/");

        kMeansClusterJob.setMapperClass(KMeansCluster.KMeansClusterMapper.class);
        kMeansClusterJob.setMapOutputKeyClass(Text.class);
        kMeansClusterJob.setMapOutputValueClass(IntWritable.class);

        kMeansClusterJob.setNumReduceTasks(0);

        FileInputFormat.addInputPath(kMeansClusterJob, new Path(sourcePath));
        FileOutputFormat.setOutputPath(kMeansClusterJob, new Path(outputPath + "/clusteredInstances" + "/"));

        kMeansClusterJob.waitForCompletion(true);
        System.out.println("finished!");
    }

    public void generateInitialCluster(){
        ClusterGenerator generator = new ClusterGenerator(conf, sourcePath, k);
        generator.generateInitialCluster(outputPath + "/");
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
        System.out.println("start");
        Configuration conf = new Configuration();
        int k = Integer.parseInt(args[0]);
        int iterationNum = Integer.parseInt(args[1]);
        String sourcePath = args[2];
        String outputPath = args[3];
        myKMeans driver = new myKMeans(k, iterationNum, sourcePath, outputPath, conf);
        driver.generateInitialCluster();
        System.out.println("initial cluster finished");
        driver.clusterCenterJob();
        driver.KMeansClusterJob();
    }
}

// 散点类
class PointInstance implements Writable {
    ArrayList<Double> value;

    public PointInstance(){
        value = new ArrayList<Double>();
    }
    public PointInstance(String line){
        // 读取并存储点坐标
        String[] valueString = line.split(" "); //点坐标按空格分割
        value = new ArrayList<Double>();
        for (String s : valueString) {
            value.add(Double.parseDouble(s));
        }
    }
    public PointInstance(PointInstance point){
        value = new ArrayList<Double>();
        value.addAll(point.getValue());
    }
    public PointInstance(int k){
        // 初始化聚类中心，k=number of clusters
        value = new ArrayList<Double>();
        for(int i = 0; i < k; i++){
            value.add(0.0);
        }
    }

    public ArrayList<Double> getValue(){
        return value;
    }

    public PointInstance add(PointInstance point){
        // 向list添加点
        if(value.size() == 0)
            return new PointInstance(point);
        else if(point.getValue().size() == 0)
            return new PointInstance(this);
//        else if(value.size() != point.getValue().size())
        else{
            PointInstance result = new PointInstance();
            for(int i = 0;i < value.size(); i++){
                result.getValue().add(value.get(i) + point.getValue().get(i));
            }
            return result;
        }
    }

    public PointInstance multiply(double num){
        PointInstance result = new PointInstance();
        for (Double aDouble : value) {
            result.getValue().add(aDouble * num);
        }
        return result;
    }

    public PointInstance divide(double num){
        PointInstance result = new PointInstance();
        for (Double aDouble : value) {
            result.getValue().add(aDouble / num);
        }
        return result;
    }

    public String toString(){
        StringBuilder s = new StringBuilder();
        for(int i = 0;i < value.size() - 1; i++){
            s.append(value.get(i)).append(" ");
        }
        s.append(value.get(value.size() - 1));
        return s.toString();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // TODO Auto-generated method stub
        out.writeInt(value.size());
        for (Double aDouble : value) {
            out.writeDouble(aDouble);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        // TODO Auto-generated method stub
        int size;
        value = new ArrayList<Double>();
        if((size = in.readInt()) != 0){
            for(int i = 0; i < size; i++){
                value.add(in.readDouble());
            }
        }
    }
}


// KMeans簇类
class Cluster implements Writable{
    private int cluster_id;
    private long num_points;
    private PointInstance center;

    public Cluster(){  //默认初始化
        this.setClusterID(-1);
        this.setNumOfPoints(0);
        this.setCenter(new PointInstance());
    }

    public Cluster(int cluster_id,PointInstance center){  //设置归属簇
        this.setClusterID(cluster_id);
        this.setNumOfPoints(0);
        this.setCenter(center);
    }

    public Cluster(String line){
        String[] value = line.split(" ");
        cluster_id = Integer.parseInt(value[0]);
        num_points = Long.parseLong(value[1]);
        center = new PointInstance(value[2]+" "+value[3]);
    }

    public String toString(){
        return cluster_id + " "
                + num_points + " " + center.toString();
    }

    public int getClusterID() {
        return cluster_id;
    }

    public void setClusterID(int cluster_id) {
        this.cluster_id = cluster_id;
    }

    public long getNumOfPoints() {
        return num_points;
    }

    public void setNumOfPoints(long num_points) {
        this.num_points = num_points;
    }

    public PointInstance getCenter() {
        return center;
    }

    public void setCenter(PointInstance center) {
        this.center = center;
    }

    public void observePoint(PointInstance point){
        PointInstance sum = center.multiply(num_points).add(point);
        num_points++;
        center = sum.divide(num_points);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // TODO Auto-generated method stub
        out.writeInt(cluster_id);
        out.writeLong(num_points);
        center.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        // TODO Auto-generated method stub
        cluster_id = in.readInt();
        num_points = in.readLong();
        center.readFields(in);
    }
}


// 生成簇，含初始簇生成
class ClusterGenerator {
    private int k;
    private FileStatus[] file_list;
    private FileSystem fs;
    private ArrayList<Cluster> k_clusters;
    private Configuration conf;

    public ClusterGenerator(Configuration conf,String filePath,int k){
        this.k = k;
        try {
            fs = FileSystem.get(URI.create(filePath),conf);
            file_list = fs.listStatus((new Path(filePath)));  //输入源文件列表
            k_clusters = new ArrayList<Cluster>(k);
            this.conf = conf;
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }
    public void generateInitialCluster(String dest_path){
        Text line = new Text();
        FSDataInputStream fsi = null;
        try {
            for (FileStatus fileStatus : file_list) {  //读取每个文件
                fsi = fs.open(fileStatus.getPath());
                LineReader lineReader = new LineReader(fsi, conf);
                while (lineReader.readLine(line) > 0) {
                    //判断是否应该加入到中心集合中去
                    System.out.println("read a line:" + line);
                    PointInstance point = new PointInstance(line.toString());
                    makeDecision(point);
                }
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            try {
                fsi.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

        }
        writeBackToFile(dest_path);
    }


    public void makeDecision(PointInstance point){
        if(k_clusters.size() < k){
            Cluster cluster = new Cluster(k_clusters.size() + 1, point);
            k_clusters.add(cluster);
        }else{
            int choice = randomChoose(k);
            if(choice != -1){  // 新簇中心
                int id = k_clusters.get(choice).getClusterID();
                k_clusters.remove(choice);
                Cluster cluster = new Cluster(id, point);
                k_clusters.add(cluster);
            }
        }
    }

    // 以1/(1+k)的概率返回一个[0,k-1]中的正整数,以k/k+1的概率返回-1.
    public int randomChoose(int k){
        Random random = new Random();
        if(random.nextInt(k + 1) == 0){
            return new Random().nextInt(k);
        }else
            return -1;
    }

    public void writeBackToFile(String dest_path){
        Path path = new Path(dest_path + "cluster-0/clusters");
        FSDataOutputStream fsi = null;
        try {
            fsi = fs.create(path);
            for(Cluster cluster : k_clusters){
                fsi.write((cluster.toString() + "\n").getBytes());
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            try {
                fsi.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

    }
}


class KMeansCluster {
    public static class KMeansClusterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private ArrayList<Cluster> kClusters = new ArrayList<Cluster>();

        @Override
        protected void setup(Context context) throws IOException,InterruptedException{
            super.setup(context);
            FileSystem fs = FileSystem.get(context.getConfiguration());
            FileStatus[] fileList = fs.listStatus(new Path(context.getConfiguration().get("clusterPath")));
            BufferedReader in = null;
            FSDataInputStream fsi = null;
            String line;
            for (FileStatus fileStatus : fileList) {
                if (!fileStatus.isDirectory()) {
                    fsi = fs.open(fileStatus.getPath());
                    in = new BufferedReader(new InputStreamReader(fsi, StandardCharsets.UTF_8));
                    while ((line = in.readLine()) != null) {
                        System.out.println("read a line:" + line);
                        Cluster cluster = new Cluster(line);
                        cluster.setNumOfPoints(0);
                        kClusters.add(cluster);
                    }
                }
            }
            in.close();
            fsi.close();
        }

        @Override
        public void map(LongWritable key, Text value, Context context)throws
                IOException, InterruptedException{
            PointInstance point = new PointInstance(value.toString());
            int id;
            try {
                id = getNearest(point);
                if(id == -1)
                    throw new InterruptedException("id == -1");
                else{
                    context.write(value, new IntWritable(id));
                }
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        public int getNearest(PointInstance point) throws Exception{
            int id = -1;
            double distance = Double.MAX_VALUE;
            Distance<Double> distanceMeasure = new EuclideanDistance<Double>();
            double newDis;
            for(Cluster cluster : kClusters){
                newDis = distanceMeasure.getDistance(cluster.getCenter().getValue()
                        , point.getValue());
                if(newDis < distance){
                    id = cluster.getClusterID();
                    distance = newDis;
                }
            }
            return id;
        }
    }
}


class KMeansAlgorithm {
    public static class KMeansMapper extends Mapper<LongWritable, Text,IntWritable,Cluster> {
        private ArrayList<Cluster> kClusters = new ArrayList<Cluster>();

        @Override
        protected void setup(Context context) throws IOException,InterruptedException{
            super.setup(context);
            FileSystem fs = FileSystem.get(context.getConfiguration());
            FileStatus[] fileList = fs.listStatus(new Path(context.getConfiguration().get("clusterPath")));
            BufferedReader in = null;
            FSDataInputStream fsi = null;
            String line = null;
            for (FileStatus fileStatus : fileList) {
                if (!fileStatus.isDirectory()) {
                    fsi = fs.open(fileStatus.getPath());
                    in = new BufferedReader(new InputStreamReader(fsi, StandardCharsets.UTF_8));
                    while ((line = in.readLine()) != null) {
                        System.out.println("read a line:" + line);
                        Cluster cluster = new Cluster(line);
                        cluster.setNumOfPoints(0);
                        kClusters.add(cluster);
                    }
                }
            }
            in.close();
            fsi.close();
        }

        @Override
        public void map(LongWritable key, Text value, Context context)throws
                IOException, InterruptedException{
            PointInstance point = new PointInstance(value.toString());
            int id;
            try {
                id = getNearest(point);
                if(id == -1)
                    throw new InterruptedException("id == -1");
                else{
                    Cluster cluster = new Cluster(id, point);
                    cluster.setNumOfPoints(1);
                    System.out.println("cluster that i emit is:" + cluster);
                    context.write(new IntWritable(id), cluster);
                }
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        // 寻找最近簇
        public int getNearest(PointInstance point) throws Exception{
            int id = -1;
            double distance = Double.MAX_VALUE;
            Distance<Double> distanceMeasure = new EuclideanDistance<Double>();
            double newDis;
            for(Cluster cluster : kClusters){
                newDis = distanceMeasure.getDistance(cluster.getCenter().getValue()
                        , point.getValue());
                if(newDis < distance){
                    id = cluster.getClusterID();
                    distance = newDis;
                }
            }
            return id;
        }

        public Cluster getClusterByID(int id){
            for(Cluster cluster : kClusters){
                if(cluster.getClusterID() == id)
                    return cluster;
            }
            return null;
        }
    }

    public static class KMeansCombiner extends Reducer<IntWritable,Cluster,IntWritable,Cluster> {
        public void reduce(IntWritable key, Iterable<Cluster> value, Context context)throws
                IOException, InterruptedException{
            PointInstance point = new PointInstance();
            int numOfPoints = 0;
            for(Cluster cluster : value){
                numOfPoints += cluster.getNumOfPoints();
                System.out.println("cluster is:" + cluster);
                point = point.add(cluster.getCenter().multiply(cluster.getNumOfPoints()));
            }
            Cluster cluster = new Cluster(key.get(),point.divide(numOfPoints));
            cluster.setNumOfPoints(numOfPoints);
            context.write(key, cluster);
        }
    }

    public static class KMeansReducer extends Reducer<IntWritable,Cluster, NullWritable,Cluster>{
        public void reduce(IntWritable key, Iterable<Cluster> value, Context context)throws
                IOException, InterruptedException{
            PointInstance point = new PointInstance();
            int numOfPoints = 0;
            for(Cluster cluster : value){
                numOfPoints += cluster.getNumOfPoints();
                point = point.add(cluster.getCenter().multiply(cluster.getNumOfPoints()));
            }
            Cluster cluster = new Cluster(key.get(),point.divide(numOfPoints));
            cluster.setNumOfPoints(numOfPoints);
            context.write(NullWritable.get(), cluster);
        }
    }
}

interface Distance<T> {
    double getDistance(List<T> a, List<T> b) throws Exception;
}

class EuclideanDistance<T extends Number> implements Distance<T> {

    @Override
    public double getDistance(List<T> a, List<T> b) throws Exception {
        // TODO Auto-generated method stub
        if(a.size() != b.size())
            throw new Exception("size not compatible!");
        else{
            double sum = 0.0;
            for(int i = 0;i < a.size();i++){
                sum += Math.pow(a.get(i).doubleValue() - b.get(i).doubleValue(), 2);
            }
            sum = Math.sqrt(sum);
            return sum;
        }
    }

}

