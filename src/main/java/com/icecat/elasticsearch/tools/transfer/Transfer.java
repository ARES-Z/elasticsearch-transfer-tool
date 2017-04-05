package com.liepin.tools;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;

public class Transfer {
    public static void main(String[] args) throws IOException {
        String inputFile = "/Users/ares/Downloads/result-online/17";
        String outputFile = "/Users/ares/Downloads/result-online/17-new";
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(inputFile)));
        PrintWriter writer = new PrintWriter(outputFile);
        String line = null;
        while (true) {
            line = reader.readLine();
            if (line == null) {
                reader.close();
                writer.flush();
                writer.close();
                break;
            }
            writer.println(line.replace("\001", "\t"));
        }
        // System.out.println(1110001l << 8);
    }
}
