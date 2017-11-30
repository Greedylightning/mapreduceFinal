/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.crimehitmap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

/**
 *
 * @author dongqi
 */
public class Location implements Writable,WritableComparable<Location>{
    
    private String lng;
    private String lat;
    private int count;

    public Location(String lng, String lat) {
        this.lng = lng;
        this.lat = lat;
    }

    public Location() {
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }
    
    
    
    public String getLng() {
        return lng;
    }

    public void setLng(String lng) {
        this.lng = lng;
    }

    public String getLat() {
        return lat;
    }

    public void setLat(String lat) {
        this.lat = lat;
    }

    @Override
    public void write(DataOutput d) throws IOException {
        WritableUtils.writeString(d, lat);
	WritableUtils.writeString(d, lng);
        WritableUtils.writeVInt(d, count);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        lat = WritableUtils.readString(di);
        lng = WritableUtils.readString(di);
        count = WritableUtils.readVInt(di);
    }

    @Override
    public int compareTo(Location o) {
        int result = lat.compareTo(o.getLat());
        if(result == 0){
            result = lng.compareTo(o.getLng());
        }
        return result;
    }
    
    @Override
    public String toString(){
        return new StringBuffer().append("{location: new google.maps.LatLng(").append(lat).append(", -").append(lng).append("), weight: ").append(count).append("},").toString();
    }
}
