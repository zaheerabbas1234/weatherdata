package com.weather;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WeatherWritable implements Writable{
	
	private DoubleWritable temperature;
    private DoubleWritable humidity;
    private DoubleWritable pressure;
    private IntWritable count;

    public WeatherWritable() {
        this.temperature = new DoubleWritable();
        this.humidity = new DoubleWritable();
        this.pressure = new DoubleWritable();
        this.count = new IntWritable();
    }

    public WeatherWritable(double temperature, double humidity, double pressure, int count) {
        this.temperature = new DoubleWritable(temperature);
        this.humidity = new DoubleWritable(humidity);
        this.pressure = new DoubleWritable(pressure);
        this.count = new IntWritable(count);
    }

    public DoubleWritable getTemperature() {
        return temperature;
    }

    public DoubleWritable getHumidity() {
        return humidity;
    }

    public DoubleWritable getPressure() {
        return pressure;
    }

    public IntWritable getCount() {
        return count;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        temperature.readFields(in);
        humidity.readFields(in);
        pressure.readFields(in);
        count.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        temperature.write(out);
        humidity.write(out);
        pressure.write(out);
        count.write(out);
    }

}
