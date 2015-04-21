# vmq_bench - simple MQTT benchmark

## Installation
    
    ./rebar get-deps
    ./rebar compile

## Running a Benchmark

    erl -pa ebin -pa deps/*/ebin -s vmq_bench -scenario sample.scenario

## Writing Scenario Files

TODO!

## Generate Plots

Make sure that you have R installed

    ./plot.r

This will produce a stats summary similar to this:
![stats summary](https://github.com/dergraf/vmq_bench/raw/master/summary.png "Stats Summary")
