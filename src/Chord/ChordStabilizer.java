package Chord;

import java.util.concurrent.TimeUnit;

public class ChordStabilizer implements Runnable {
	
	Chord chord;

	public ChordStabilizer(Chord c) {
		this.chord = c;
	}
	
	@Override
	public void run() {
		
		System.out.println("Running Stabilizer...");
		
		this.chord.stabilize();
		this.chord.fix_fingers();
		this.chord.fix_sucessor();
		
		this.chord.printKnowns();
		
		this.chord.getPeer().getExecutor().schedule(this, Chord.UPDATE_TIME, TimeUnit.SECONDS);
	}

}
