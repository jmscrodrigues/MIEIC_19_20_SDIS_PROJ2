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
		try {
			this.chord.fix_fingers();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		this.chord.printKnowns();
		
		this.chord.getPeer().getExecuter().schedule(this, Chord.UPDATE_TIME, TimeUnit.SECONDS);
	}

}
