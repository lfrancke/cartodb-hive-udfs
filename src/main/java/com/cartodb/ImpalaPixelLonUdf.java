package com.cartodb;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;

public class ImpalaPixelLonUdf extends UDF {

  public static final int TILE_SIZE = 256;

  public static void main(String[] args) {
    double lat = 33.5738716125;
    double lon = 130.401321411;

    ImpalaPixelLonUdf udf = new ImpalaPixelLonUdf();
    double point = udf.mercatorPoint(udf.mercator(lon), TILE_SIZE);
    int res = 1 << 17;
    int pixelX = (int) Math.floor(point * res);
    System.out.println(pixelX);
  }

  public double mercatorPoint(double x, int tileSize) {
    int t2 = tileSize / 2;

    return t2 + x * tileSize;
  }

  public double mercator(double lon) {
    return lon / 360.0;
  }

  public long evaluate(DoubleWritable lon, IntWritable zoom) {
    double x = mercatorPoint(mercator(lon.get()), TILE_SIZE);
    int res = 1 << zoom.get();
    int pixelX = (int) Math.floor(x * res);
    return pixelX;
  }



}
