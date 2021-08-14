package com.github.vitalibo.glue.job;

import com.github.vitalibo.glue.Job;
import com.github.vitalibo.glue.Spark;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class ExampleJob implements Job {

    @Override
    public void process(Spark spark) {
        System.out.println("Process Example Job");
    }

}
