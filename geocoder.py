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

    search_SQL = "SELECT locale FROM locales WHERE result IS NULL"

    notfound_SQL = "UPDATE locales SET result='None', result_count=0 WHERE locale=\'%s\'"
    manyfound_SQL = "UPDATE locales SET longitude=%s, latitude=%s, way=ST_GeographyFromText(\'POINT(%s %s)\'), result=%s, result_count=%i WHERE locale=\'%s\'"
    onefound_SQL = "UPDATE locales SET longitude=%s, latitude=%s, way=ST_GeographyFromText(\'POINT(%s %s)\'), result=%s, result_count=1 WHERE locale=\'%s\'"
    time_SQL = "UPDATE locales SET source=\'%s\', geocoded=\'%s\' WHERE locale=\'%s\'"


    source = "GeoNames"
    gn = geocoders.GeoNames()

    cur.execute(search_SQL)
    for i, row in enumerate(cur):
        locale,= row

        t = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        update.execute(time_SQL % (source, t, locale.replace("'","''")))
        
        result = gn.geocode(locale)
        if result == None:
            print i, "Unfound location %s" % locale
            update.execute(notfound_SQL)
            
        elif type(result) == list:
            print i, "Multiple locations %s" % locale
            result_count = len(result)
            _, (lat, lng) = result[0]
            update.execute(manyfound_SQL % (str(lng),str(lat),str(lng),str(lat), repr(result), result_count, str(locale)))
            
        elif type(result) == tuple:
            _, (lat, lng) = result
            if lat < -90 or lat > 90 or lng < -180 or lng >180:
                print i, "Out of range %s" % locale

            print "Found %s" % locale
            update.execute(onefound_SQL % (str(lng),str(lat),str(lng),str(lat), repr(result), str(locale)))
        else:
            print i, "Results not recognized %s" % locale

        database.commit()

    cur.close()
    database.commit()
    database.close()
        
if __name__ == "__main__":
    args = {}
    execute(args)
