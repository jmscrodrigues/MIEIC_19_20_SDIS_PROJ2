package Chord;

import java.util.concurrent.TimeUnit;

public class ChordStabilizer implements Runnable{
	
	Chord chord;

	public ChordStabilizer(Chord c) {
		this.chord = c;
	}
	
	@Override
	public void run() {
		
		System.out.println("Running Stabilize");
		
		this.chord.stabilize();
		this.chord.fix_fingers();
		
		this.chord.printKnowns();
		
		this.chord.getPeer().getExecuter().schedule(this, Chord.UPDATE_TIME, TimeUnit.SECONDS);
	}

}
