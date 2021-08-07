package com.github.vitalibo.glue;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class Spark {

    public void submit(Job job) {
        job.process(this);
    }

}
