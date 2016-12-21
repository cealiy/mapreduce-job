package com.cealiy.main;

import com.cealiy.request.cupid.RequestStatistics;

public class JobClient {
	
	
	public static void main(String[] args) throws Exception{
		if(args==null||args.length<1){
			return;
		}
		if(args[0].equals("statistics-cupid-request")){
			if(args.length<3){
				return;
			}
			RequestStatistics.statistics(args[1], args[2]);
		}
	}

}
