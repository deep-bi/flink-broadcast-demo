package bi.deep.flink.demo;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Random;

public class EventGenerator implements Iterator<InputEvent>, Serializable {
    private int id = 0;
    private final Random rand = new Random();

    @Override
    public boolean hasNext() {
        return true;
    }

    @Override
    public InputEvent next() {
        try {
            Thread.sleep(100L);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return new InputEvent(String.valueOf(id++), (rand.nextBoolean()) ? "1" : "2", (rand.nextBoolean()) ? "1" : "2");
    }
}