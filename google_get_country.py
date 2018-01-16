#!/usr/bin/env python

import os,sys
from urllib2 import urlopen
import argparse
import pymysql as MySQLdb
from geopy.geocoders import Nominatim
import geocoder
geolocator = Nominatim()
from pygeocoder import Geocoder
import pandas as pd
import numpy as np
#page = urlopen("https://docs.python.org/3/howto/urllib2.html")
#contents = page.read()
import json

def get_countries_from_db(args):
    country_query = "SELECT term_id, term_name from term where ontology_id ='4'"
    args.cur.execute(country_query)
    countries = {}
    for row in args.cur.fetchall():
        countries[row[1]] = row[0]    
    print countries
    return countries
    
def go(args):
    
    country_update_queries = []
    google_earth_latlon_pairs =[]
    with open(args.infile,'r') as data_file:
        for line in data_file:
            if not line:
                continue
            items = line.strip().split()
            if items[0] == 'project':
                continue
            lat = items[4]
            lon = items[5]
            did = items[3]
            #print 'lat,lon,did',lat,lon,did
            #print 'Denmark',args.countries['Denmark']
            #print getplace(41.5265, -70.6731)
            country = 'XXX'
            #location = Geocoder.reverse_geocode(float(lat), float(lon))
            #location = geolocator.reverse((lat,lon))   #("52.509669, 13.376294")
            mapboxkey = 'pk.eyJ1IjoiYXZvb3JoaXMiLCJhIjoiY2o2ejVhZTVkMjZ1dDM3bno2YmxwY28zNSJ9.RB6bnNPQcRlI62cy4XSZMQ'
            # g = geocoder.mapbox([float(lat), float(lon)], method='reverse')
#             print g
            try:
                #location = Geocoder.reverse_geocode(float(lat), float(lon))
                g = geocoder.mapbox([float(lat), float(lon)], method='reverse', key=mapboxkey)
                country = g.country
                #location = geolocator.reverse((lat,lon))   #("52.509669, 13.376294")
                #print location
                #print lat,lon,'  '+str(getplace(lat, lon))
                #country = getplace(float(lat), float(lon))
                #q = "UPDATE required_metadata_info set geo_loc_name_id='' where dataset_id=''"
                gotit = True
            except:
               google_earth_latlon_pairs.append(lat+','+lon+'\t'+did)
               gotit = False
               
               
            if gotit:
                cid = '0'
                try:
                    cid = str(args.countries[country])
                    country_update_queries.append("UPDATE required_metadata_info set geo_loc_name_id='"+cid+"' where dataset_id='"+did+"';")
                except:
                    print 'couldnt find country:',country
                    google_earth_latlon_pairs.append(lat+','+lon+'\t'+did)
                #print country, cid,lat,lon
                
               # print lat,lon
            #location = geolocator.reverse((lat,lon))   #("52.509669, 13.376294")
            #print(location.address)
            
            # print 'lat '+lat+ ' lon '+lon
#             try:
#                 print '  '+str(getplace(lat, lon))
#             except:
#                 print '  error (water?)'
    
    for n in country_update_queries:
        print n
    for n in google_earth_latlon_pairs:
        print n
def getplace(lat, lon):
    url = "https://maps.googleapis.com/maps/api/geocode/json?"
    url += "latlng=%s,%s&sensor=false&key=%s" % (lat, lon, 'AIzaSyCuH59zVfzdiBB90bZxwkDLi7nvWZDFE_E')
    print url
    v = urlopen(url).read()
    print v
    j = json.loads(v)
    components = j['results'][0]['address_components']
    country = town = None
    for c in components:
        if "country" in c['types']:
            country = c['long_name']
        if "postal_town" in c['types']:
            town = c['long_name']
    return country
# 
# 
# print(getplace(51.1, 0.1))
# print(getplace(51.2, 0.1))
# print(getplace(51.3, 0.1))

if __name__ == '__main__':
    myusage = """
        Usage: ./google_get_country.py -in infile
        format infile:
            DCO_BKR_Av4v5	Aar_59E_25H2	        300	238914	55.004821	10.108177
            DCO_BKR_Av4v5	Aar_DrillFluid_59E_NC	300	238915	55.004821	10.108177
            
    Use:
    select project, dataset, project_id, dataset_id, latitude, longitude from required_metadata_info
        join dataset using (dataset_id)
        join project using(project_id)
        where project like 'DCO%'
        and latitude is not null


    """
    parser = argparse.ArgumentParser(description="", usage=myusage)
    parser.add_argument("-in","--in",                   
                required=True,  action="store",   dest = "infile", default='',
                help="""ProjectID (used with -add) no response if -list also included""") 
    parser.add_argument("-host", "--host",    
                required=False,  action='store', dest = "dbhost",  default='localhost',
                help="choices=['vampsdb','vampsdev','localhost']") 
                
                    
    if len(sys.argv[1:]) == 0:
        print myusage
        sys.exit() 
    args = parser.parse_args()
    if args.dbhost == 'vamps' or args.dbhost == 'vampsdb':
        args.NODE_DATABASE = 'vamps2'
        dbhost = 'vampsdb'        
    elif args.dbhost == 'vampsdev':
        args.NODE_DATABASE = 'vamps2'
        dbhost = 'bpcweb7'
    else:
        dbhost = 'localhost'
        args.NODE_DATABASE = 'vamps_development'
    args.db = MySQLdb.connect(host=dbhost, db=args.NODE_DATABASE, # your host, usually localhost
                             read_default_file="~/.my.cnf_node"  )
    args.cur = args.db.cursor()
    args.countries = get_countries_from_db(args)
    go(args)    
    