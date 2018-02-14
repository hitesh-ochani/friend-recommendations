package recommendsFriend.RecommendsFriend;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Random;
import java.util.Scanner;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

  public class My_Mapper
       extends Mapper<Object, Text, IntWritable, Text>{

    private final static IntWritable mapKeyOut  = new IntWritable(1);
    private final static Text mapValueOut = new Text("");
    int ColumnsInSecond = 1;
    int RowsInSecond = 10;
    boolean didIReadVector = false;
    private Text word = new Text();
    private int tempProduct=0;
    int Matrix [][] = new int[RowsInSecond][ColumnsInSecond];
	  Text ValueOut = new Text();

    public void map(Object key, Text value, Context context
            ) throws IOException, InterruptedException {
	
	//System.out.println("\n\n\nEnters a new mapper");
	Scanner scanner = new Scanner(value.toString());
	while (scanner.hasNextLine()) {
	  String line = scanner.nextLine();
	// 	  System.out.println("->"+line);
	   //StringTokenizer itr = new StringTokenizer(line.toString());
	   
	   String[] key_value = line.split("\\s+");
	  // System.out.println(key_value.length);
	   if(key_value.length == 1) {
		   //Person has no friend, I am ignoring this person
		   String my_key = key_value[0];

		   ValueOut.set(my_key+"_"+"-2");
		   mapKeyOut.set(Integer.parseInt(my_key));
		  // System.out.println("The one with no friends"+my_key);
		   context.write(mapKeyOut,ValueOut);
		   
	   } else {
		   // Person has only one friend

		   //key_value.length
		 //  System.out.println(key_value.toString());
		  //System.out.println("->"+tokens[0]+tokens[1]+tokens[2]+tokens[3]);
		   String my_key = key_value[0];
		

		   String[] values = key_value[1].split(",");
		   
		   
		   
	//	   System.out.println("Values are : " + values[0].toString());
		//  int totalCombination = values.length*(values.length - 1);

		  for(int i = 0; i<values.length;i++) {
			 // values[i] is recommended friend
			  mapKeyOut.set(Integer.parseInt(values[i]));

			  for(int j=0; j<values.length;j++) {
				  if (i!=j) {
					  ValueOut.set(values[j]+"_"+my_key);
					  context.write(mapKeyOut,ValueOut);
					 // System.out.println(KeyOut.toString()+ "->"+ValueOut.toString());
				  }
			  }
			  ValueOut.set(my_key+"_"+"-1");
			  context.write(mapKeyOut,ValueOut);
			//  System.out.println(KeyOut.toString()+ "->"+ValueOut.toString());

			  
		  }

		  
	   }
	  
	}
	scanner.close();
	
	
	// 	System.out.println(value);
	
	
	
	
	}
}
