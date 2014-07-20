package example;
import java.util.*;
import java.util.regex.*;

import io.FixValue;
import io.LongWritable;
import io.Text;
import mapred.Context;
import mapred.Mapper;
public class NGramGenerationMapper extends Mapper {
	FixValue one = new FixValue(1); 
    @Override
    public void map(LongWritable key, Text value, Context context) {
 	
		for (int i = 0; i < 5; i++) {
			String strList = handle(value.toString());
			for (String str : ngrams(i, strList)) {
				context.write(new Text(str), one);
			}
		}
    }
    
    private String handle(String s) {
		String reg1 = "[^a-zA-Z]";
		Pattern p1 = Pattern.compile(reg1);
		Matcher m1 = p1.matcher(s);
		String s1 = m1.replaceAll(" ").trim();

		String reg2 = "\\s{2,}";
		Pattern p2 = Pattern.compile(reg2);
		Matcher m2 = p2.matcher(s1);
		String s2 = m2.replaceAll(" ").trim();
		return s2.toLowerCase();
	}

	public static List<String> ngrams(int n, String str) {
		List<String> wordsList = new ArrayList<String>();
		String[] words = str.split(" ");
		for (int i = 0; i < words.length - n + 1; i++){
			StringBuilder tmp = new StringBuilder();
			for (int k = i; k < i+n; k++) {
				if (tmp.length() > 0)
					tmp.append(" ");
				tmp.append(words[k]);
			}
			wordsList.add(new String(tmp));
		}
		return wordsList;
	}

	public static String concat(String[] words, int i, int j) {
		StringBuilder str = new StringBuilder();
		for (int k = i; k < j; k++) {
			if (str.length() > 0)
				str.append(" ");
			str.append(words[k]);
		}
		return str.toString();
	}
  }
 
