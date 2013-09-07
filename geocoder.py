import psycopg2
from geopy import geocoders
import os
import datetime

def execute(args):
    dbase_file_name = os.path.join(os.path.dirname(os.path.abspath(__file__)), "flickr.db")

    dbase_file = open(dbase_file_name, "r")
    dbase_post_gis = dbase_file.read().strip().replace("\n", " ")
    dbase_file.close()

    database = psycopg2.connect(dbase_post_gis)
    cur = database.cursor()
    update = database.cursor()

    search_SQL = "SELECT locale FROM locales WHERE geocoded IS NULL"
    update_SQL = "UPDATE locales SET longitude=%d, latitude=%d, way=ST_GeographyFromText(\'POINT(%d %d)\') WHERE locale=\'%s\'"
    time_SQL = "UPDATE locales SET source=\'%s\', geocoded=\'%s\' WHERE locale=\'%s\'"


    source = "GeoNames"
    gn = geocoders.GeoNames()

    cur.execute(search_SQL)
    for i, row in enumerate(cur):
        locale,= row
        locale = locale.replace("'","''")
        if locale.count("/") < 1:
            try:
                t = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                update.execute(time_SQL % (source, t, locale))
                database.commit()
                
                place, (lat, lng) = gn.geocode(locale, exactly_one=True)
                if lat < -90 or lat > 90 or lng < -180 or lng >180:
                    print i, "Out of range %s" % locale
                else:
                    print update_SQL % (lng,lat,lng,lat, locale, t)
                    update.execute(update_SQL % (lng,lat,lng,lat, locale), t)
                    database.commit()
                    print i, "Geocoded %s" % locale
            except TypeError:
                print i, "Unfound location %s" % locale
            except ValueError:
                print i, "Multiple locations %s" % locale
        else:
            print i, "Unfriendly name %s" % locale

    cur.close()
    database.commit()
    database.close()
        
if __name__ == "__main__":
    args = {}
    execute(args)
