package com.weather;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
public class WeatherReducer extends Reducer<Text, WeatherWritable, Text, Text>{
	
	@Override
    public void reduce(Text key, Iterable<WeatherWritable> values, Context context)
            throws IOException, InterruptedException {

        double tempSum = 0;
        double humiditySum = 0;
        double pressureSum = 0;
        int count = 0;

        // Accumulate the weather data for each location
        for (WeatherWritable val : values) {
            tempSum += val.getTemperature().get();
            humiditySum += val.getHumidity().get();
            pressureSum += val.getPressure().get();
            count += val.getCount().get();
        }

        // Calculate averages
        double avgTemperature = tempSum / count;
        double avgHumidity = humiditySum / count;
        double avgPressure = pressureSum / count;

        // Output the result in the format: locationId \t avgTemperature, avgHumidity, avgPressure
        context.write(key, new Text(avgTemperature + "," + avgHumidity + "," + avgPressure));
    }


}
