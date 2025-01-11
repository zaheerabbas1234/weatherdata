package com.weather;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WeatherMapper extends Mapper<Object, Text, Text, WeatherWritable>{
	
	@Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Input format: 2025-01-11 12:00,location_id=1234,temperature=25.3,humidity=78.2,pressure=1012
        String line = value.toString();
        String[] fields = line.split(",");

        try {
            // Extract location ID
            String locationId = fields[1].split("=")[1]; // location_id=1234 -> locationId = 1234

            // Extract weather data
            double temperature = Double.parseDouble(fields[2].split("=")[1]); // temperature=25.3
            double humidity = Double.parseDouble(fields[3].split("=")[1]); // humidity=78.2
            double pressure = Double.parseDouble(fields[4].split("=")[1]); // pressure=1012

            // Write the output as (locationId, WeatherWritable)
            context.write(new Text(locationId), new WeatherWritable(temperature, humidity, pressure, 1));

        } catch (Exception e) {
            // Handle malformed lines or missing fields
            e.printStackTrace();
        }
    }

}
