package debug;

import java.util.Random;

public class RandomSleep extends Thread{
	Random random = new Random();
	int next = random.nextInt();
}
