package org.cron.job;

import java.util.ArrayList;
import java.util.List;

public class Transpose {
	public static void main(String args[]) {
	       String _2d[][]={
	    		   {"1","1"},
	    		   {"2","2"},
	    		   {"3","3"}
	       };
	       
	       List<ArrayList<String>> transposeList=new ArrayList<ArrayList<String>>();
	       for(String str[] : _2d){
	    	   int cols = 0;
	           for(String st: str){
	        	   ArrayList<String> rows = transposeList.size() == cols ? null:transposeList.get(cols);
	        	   if(rows ==null) {
	        		   rows = new ArrayList<String>();
	        		   transposeList.add(rows);
	        	   }
	        	   rows.add(st);
	               System.out.print("\t "+ st);
	               cols++;
	           }
	           
	           System.out.println("");
	       }
	       
	       System.out.println(transposeList);
	    }
}
