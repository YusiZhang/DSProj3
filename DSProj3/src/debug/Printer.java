package debug;

import java.util.Collection;
import java.util.Map;

public  class Printer {
	public static void printC(Collection c){
		System.out.println("Printing Collection");
		for(Object obj : c){
			System.out.println(obj.toString());
		}
	}
	
	public static void printT(Map m){
		System.out.println("Printing Map");
		for(Object obj : m.keySet()) {
			System.out.println("key: " + obj.toString() + " value: " + m.get(obj).toString());
		}
	}
}
