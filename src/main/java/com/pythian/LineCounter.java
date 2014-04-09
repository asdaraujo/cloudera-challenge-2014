package com.pythian;


import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;

import com.google.common.base.Splitter;


/**
 * Splits a line of text, filtering known stop words.
 */
public class LineCounter extends DoFn<String, String> {

    @Override
    public void process(String line, Emitter<String> emitter) {
        emitter.emit("1");
    }
}
