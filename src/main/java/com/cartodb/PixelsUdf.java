package com.cartodb;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Objects;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.LongWritable;

public class PixelsUdf extends GenericUDF {

  public static final int TILE_SIZE = 256;

  private final DoubleObjectInspector doi = PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;

  private final Object[] result = new Object[2];
  private final LongWritable xWritable = new LongWritable();
  private final LongWritable yWritable = new LongWritable();
  private boolean firstRun = true;
  private int zoom = -1;

  public static void main(String[] args) {
    double lat = 33.5738716125; // 102, 205, 410
    double lon = 130.401321411; // 220, 441, 882

    PixelsUdf udf = new PixelsUdf();
    Point point = udf.mercatorPoint(udf.mercator(lon, lat), TILE_SIZE);
    int res = 1 << 2;
    int pixelX = (int) Math.floor(point.x * res);
    int pixelY = (int) Math.floor(point.y * res);
    System.out.println(pixelX);
    System.out.println(pixelY);
  }

  public Point mercatorPoint(Point point, int tileSize) {
    int t2 = tileSize / 2;
    return new Point(t2 + point.x * tileSize, t2 - point.y * tileSize);
  }

  public Point mercator(double lon, double lat) {
    double reprojectedLat = Math.min(Math.max(lat, -89.189), 89.189);
    reprojectedLat = Math.PI * (reprojectedLat / 180.0);
    reprojectedLat = 0.5 * Math.log(Math.tan(0.25 * Math.PI + 0.5 * reprojectedLat)) / Math.PI;

    double reprojectedLon = lon / 360.0;

    return new Point(reprojectedLon, reprojectedLat);
  }

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 3) {
      throw new UDFArgumentLengthException("pixels() takes three arguments: lat, lon, zoom");
    }

    List<String> fieldNames = new ArrayList<>();
    List<ObjectInspector> fieldOIs = new ArrayList<>();

    if ((arguments[0].getCategory() != ObjectInspector.Category.PRIMITIVE) || !arguments[0].getTypeName()
                                                                                 .equals(serdeConstants.DOUBLE_TYPE_NAME)) {
      throw new UDFArgumentException("pixels(): lat has to be double");
    }

    if ((arguments[1].getCategory() != ObjectInspector.Category.PRIMITIVE) || !arguments[1].getTypeName()
                                                                                 .equals(serdeConstants.DOUBLE_TYPE_NAME)) {
      throw new UDFArgumentException("pixels(): lon has to be double");
    }

    if ((arguments[2].getCategory() != ObjectInspector.Category.PRIMITIVE) || !arguments[2].getTypeName()
                                                                                 .equals(serdeConstants.INT_TYPE_NAME)) {
      throw new UDFArgumentException("pixels(): zoom has to be an int");
    }

    fieldNames.add("x");
    fieldNames.add("y");
    fieldOIs.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
    fieldOIs.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
    result[0] = xWritable;
    result[1] = yWritable;

    return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    double lon = doi.get(arguments[0].get());
    double lat = doi.get(arguments[1].get());

    if (firstRun) {
      zoom = PrimitiveObjectInspectorFactory.writableIntObjectInspector.get(arguments[2].get());
      firstRun = false;
    }

    Point point = mercatorPoint(mercator(lon, lat), TILE_SIZE);
    int res = (int) Math.pow(2, zoom);
    int pixelX = (int) Math.floor(point.x * res);
    int pixelY = (int) Math.floor(point.y * res);
    xWritable.set(pixelX);
    yWritable.set(pixelY);
    return result;
  }

  @Override
  public String getDisplayString(String[] children) {
    return "pixels";
  }

  public static final class Point {

    double x;
    double y;

    private Point(double x, double y) {
      this.x = x;
      this.y = y;
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this).add("x", x).add("y", y).toString();
    }
  }

}
