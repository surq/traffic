package com.didi.util.auto;

public class GPS {

    double lng, lat;

    public GPS(double lng, double lat) {
        this.lng = lng;
        this.lat = lat;
    }

    public static double EARTH_RADIUS = 6378.137;

    public static double rad(double d) {
        return d * Math.PI / 180.0;
    }

    /**
     * 根据两点间经纬度坐标（double值），计算两点间距离，单位为公里
     *
     * @param lat1
     * @param lng1
     * @param lat2
     * @param lng2
     * @return
     */
    public static double GetDistance(double lng1, double lat1, double lng2, double lat2) {
        double radLat1 = rad(lat1);
        double radLat2 = rad(lat2);
        double a = radLat1 - radLat2;
        double b = rad(lng1) - rad(lng2);
        double s = 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(a / 2), 2) +
                Math.cos(radLat1) * Math.cos(radLat2) * Math.pow(Math.sin(b / 2), 2)));
        s = s * EARTH_RADIUS;
        return s;
    }
    
    /**
     * same latitude, see how many cross nodes on the left
     *
     * @param lng
     * @param lat
     * @return
     */
    public static boolean inPolygon(double lng, double lat, double [][] polygon) {
        int leftCross = 0;
        for (int i = 0; i < polygon.length; i++) {
            GPS startGPS = new GPS(polygon[i][0], polygon[i][1]);
            int endNodeIdx = i + 1;
            // if reach the end, next node will be the first node
            if (endNodeIdx == polygon.length) {
                endNodeIdx = 0;
            }
            GPS endGPS = new GPS(polygon[endNodeIdx][0], polygon[endNodeIdx][1]);

            // count cross nodes on the left
            if ((startGPS.lat > lat && lat > endGPS.lat) || (startGPS.lat < lat && lat < endGPS.lat)) {
                // compute lng of cross node
                double crossLng = startGPS.lng - (startGPS.lng - endGPS.lng) * (startGPS.lat - lat) / (startGPS.lat - endGPS.lat);
                if (crossLng < lng)
                    leftCross++;
            }
        }
        if (leftCross % 2 == 1)
            return true;
        else return false;
    }
}
