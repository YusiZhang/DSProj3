import config.ParseConfig;


public class Test {
	public static void main(String[] args) {
		try {
			ParseConfig config = new ParseConfig("src/ConfigFile.txt");
			System.out.println(config.MasterIP+"");
			System.out.println(config.MasterMainPort+"");
			System.out.println(config.StartPort+"");
			System.out.println(config.EndPort+"");
		} catch (Exception e) {
			
			System.out.println("Something wrong with config file" + "\n" + e.toString());
			System.exit(1);
		}
	}
}
