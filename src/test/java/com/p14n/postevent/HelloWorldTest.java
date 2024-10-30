package com.p14n.postevent;

import org.junit.jupiter.api.Test;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HelloWorldTest {
    @Test
    void testMainOutput() {
        // Capture System.out
        ByteArrayOutputStream outContent = new ByteArrayOutputStream();
        PrintStream originalOut = System.out;
        System.setOut(new PrintStream(outContent));

        try {
            // Run the main method
            HelloWorld.main(new String[]{});
            
            // Get the output and restore System.out
            String output = outContent.toString().trim();
            
            // Assert the output contains "Hello world"
            assertTrue(output.contains("Hello world"), 
                "Output should contain 'Hello world'");
        } finally {
            // Restore original System.out
            System.setOut(originalOut);
        }
    }
}
