package com.p14n.postevent;

import org.junit.jupiter.api.Test;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class MainTest {
    
    @Test
    void testMainOutput() {
        // Capture System.out
        ByteArrayOutputStream outContent = new ByteArrayOutputStream();
        PrintStream originalOut = System.out;
        System.setOut(new PrintStream(outContent));

        // Run main method
        Main.main(new String[]{});

        // Restore System.out
        System.setOut(originalOut);

        // Verify output
        assertEquals("Hello from PostEvent!" + System.lineSeparator(), outContent.toString());
    }
}
