
public class Timer {
	long startTime, endTime, elapsedTime;

	public void start() {
		startTime = System.currentTimeMillis();
	}

	public void end() {
		endTime = System.currentTimeMillis();
		elapsedTime = endTime - startTime;
	}

	public String toString() {
		return "Time: " + elapsedTime;
	}

}
