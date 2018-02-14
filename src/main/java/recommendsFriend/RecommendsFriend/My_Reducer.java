package recommendsFriend.RecommendsFriend;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

  public class My_Reducer
       extends Reducer<IntWritable,Text,IntWritable,Text> {
    private final static Text ValueOut  = new Text("");
    private double sum = 0;
    private double count = 0;
    private String[] tokens;
    private Double oneSum,oneCount;
    private int temp;
    private int[] common_friends_count;
    private String[] recommend_them;
    int testThis;
    
//    public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map) {
//        return map.entrySet()
//                  .stream()
//                  .sorted(Map.Entry.comparingByValue(/*Collections.reverseOrder()*/))
//                  .collect(Collectors.toMap(
//                    Map.Entry::getKey, 
//                    Map.Entry::getValue, 
//                    (e1, e2) -> e1, 
//                    LinkedHashMap::new
//                  ));
//    }
    
    public void reduce(IntWritable key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      // Best way is to use hashmap, where key is the each value, and hashmap's value is the count it occurred.
      // Once hashmap is ready, transfer each element to some sortable type (maybe a different hashmap) where key is the value and value is the key.
      //use the below

    	common_friends_count = new int[1000];
    	recommend_them = new String[1000];

      int sum = 0;
      HashMap<String,Integer>  hm=new HashMap<String,Integer>();  
      
     //SortedSet<Integer> all_counts = new TreeSet<Integer>(hm.values());
      //for (Integer my_count : all_counts) { 
        // String value = map.get(key);
         // do something
      //}
      //hm.values()
      
  	   Iterator<Text> itr = values.iterator();
  	   while (itr.hasNext()) {
  	    String text = itr.next().toString();
  	    tokens = text.split("_");
  	 //   System.out.println("Tokens are "+tokens[0]+"_"+tokens[1]);

  	    
  	    if(!hm.containsKey(tokens[0])){
  	    	if(tokens[1].equalsIgnoreCase("-2")) {	//Checking if it is -2
	    			hm.put(tokens[0], -2);		//for the person with no friends.

  	    		} else if(!tokens[1].equalsIgnoreCase("-1")) {
  	    			hm.put(tokens[0], 1);		//initialize with one

  	    		} else {
  	    			hm.put(tokens[0], -1);		//initialize with -1 to keep track of friends
  	    		}
	    		
	   // 		System.out.println(hm.toString());
  	    } else {
  	    		if(tokens[1].equalsIgnoreCase("-2")) {	//if it is -2
  	    			hm.put(tokens[0], -2);
  	    		}
  	    else if(!tokens[1].equalsIgnoreCase("-1")) {	//Checking if it is not -1
  	    			temp = hm.get(tokens[0]);
  	    			if(temp != -1) {		//Checking if it is not -1
  	    				testThis = temp + 1;	//increment only if the already present value is not -1
  	  	  	    		hm.put(	tokens[0], testThis	);
  	  	    		}
  	    			
  	  	    		
	    		} else {				// friend 
	    			hm.put(tokens[0], -1);		//overwrite
	    		}
  	    		
  	    }

    }
  	   
  	  
  	   /*
  	   //by this line we get the products in hashmap
  	   //now we sum it.
  	   for (String single : hm.values()) {
		sum = sum + Integer.parseInt(single);
	}  	   */
  	  
//  	 for(Map.Entry<String, Integer> entry : hm.entrySet()) {
//  		 
//   		System.out.println(key.toString()+" should be recommended "+entry.getKey()+" based on count : "+entry.getValue());
//
//   	}
  	 
  	 //Understood the below sort from a website where we first get hashmap entryset, then use comparator on values and use .reversed to get a reverseorder.
  	 
     Set<Map.Entry<String, Integer>> my_entries = hm.entrySet();
  	 
  	Comparator<Map.Entry<String, Integer>> valueComparator = new Comparator<Map.Entry<String,Integer>>() 
  	{ public int compare(Map.Entry<String, Integer> e1, Map.Entry<String, Integer> e2) { Integer v1 = e1.getValue(); Integer v2 = e2.getValue(); return v1.compareTo(v2); } };


  	List<Map.Entry<String, Integer>> my_listOfEntries = new ArrayList<Map.Entry<String, Integer>>(my_entries);
  	
    Collections.sort(my_listOfEntries, valueComparator.reversed());
    
    LinkedHashMap<String, Integer> sortByValue = new LinkedHashMap<String, Integer>(my_listOfEntries.size());
    
    for(Map.Entry<String, Integer> entry : my_listOfEntries){
    		sortByValue.put(entry.getKey(), entry.getValue());
    	}

    Set<Map.Entry<String, Integer>> entrySetSortedByValue = sortByValue.entrySet(); 
    String recommendations = "";
	
    int ten_values=0;

    for(Map.Entry<String, Integer> mapping : entrySetSortedByValue){
    		if(mapping.getValue() == -2){
    			recommendations="";
    		} else if(mapping.getValue() != -1) {
    			
    			if(ten_values==10) {
    				break;
    			} else {
        			recommendations = recommendations + mapping.getKey()+" ";
            	//	System.out.println(mapping.getKey() + " ==> " + mapping.getValue()); 
            		ten_values++;
    			}


    		}
    	}
    ValueOut.set(recommendations);
  	 context.write(key, ValueOut);
  	 
  	 
  	 
  	 
  		
  		
  	 

  }
}