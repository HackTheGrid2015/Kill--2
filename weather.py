import forecastio
import datetime

import csv

myfile = open("data_dec.csv",'wb')
wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)

api_key = "ffde1533d7026037bd822d42651ebd77"
lat = 37.17
lng = -83.16

x = 30

for i in xrange(1,30):
    aug_time = datetime.datetime(2014, 12 , i, 0, 0, 0)

    forecast_aug = forecastio.load_forecast(api_key, lat, lng, time = aug_time)

    byHour_aug = forecast_aug.hourly()

    for item in byHour_aug.data:
        x = item.time
        y = 0
        try:
            y = item.temperature
        except:
            y = 0
        wr.writerow([x,y])

#4.13 4.18 5.57 5.52 6.18 6.68 6.20 5.79 6.52 6.69 6.29 3.68 5.62