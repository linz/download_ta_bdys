################################################################################
#
# download_admin_bdys.py
#
# Copyright 2014 Crown copyright (c)
# Land Information New Zealand and the New Zealand Government.
# All rights reserved
#
# This program is released under the terms of the new BSD license. See the 
# LICENSE file for more information.
#
################################################################################

import os
import sys
import re
import json
import string
import socket
import urllib2
import logging.config

import socket,time
from zipfile import ZipFile
from paramiko import Transport, SFTPClient

from optparse import OptionParser
from ConfigParser import SafeConfigParser

try:
    from osgeo import ogr, osr, gdal
except:
    try:
        import ogr, osr, gdal
    except:
        sys.exit('ERROR: cannot find python OGR and GDAL modules')

version_num = int(gdal.VersionInfo('VERSION_NUM'))
if version_num < 1100000:
    sys.exit('ERROR: Python bindings of GDAL 1.10 or later required')

# make sure gdal exceptions are not silent
gdal.UseExceptions()
osr.UseExceptions()
ogr.UseExceptions()

logger = None

# translate geometry to 0-360 longitude space
def shift_geom ( geom ):
    if geom is None:
        return
    count = geom.GetGeometryCount()
    if count > 0:
        for i in range( count ):
            shift_geom( geom.GetGeometryRef( i ) )
    else:
        for i in range( geom.GetPointCount() ):
            x, y, z = geom.GetPoint( i )
            if x < 0:
                x = x + 360
            elif x > 360:
                x = x - 360
            geom.SetPoint( i, x, y, z )
    return

#check is geometry ring is clockwise.
def ring_is_clockwise(ring):
    total = 0
    i = 0
    point_count = ring.GetPointCount()
    pt1 = ring.GetPoint(i)
    pt2 = None
    for i in range(point_count-1):
        pt2 = ring.GetPoint(i+1)
        total += (pt2[0] - pt1[0]) * (pt2[1] + pt1[1])
        pt1 = pt2
    return (total >= 0)

# this is required because of a bug in OGR http://trac.osgeo.org/gdal/ticket/5538
def fix_esri_polyon(geom):
    polygons = []
    count = geom.GetGeometryCount()
    if count > 0:
        poly = None
        for i in range( count ):
            ring = geom.GetGeometryRef(i)
            if ring_is_clockwise(ring):
                poly = ogr.Geometry(ogr.wkbPolygon)
                poly.AddGeometry(ring)
                polygons.append(poly)
            else:
                poly.AddGeometry(ring)
    new_geom = None
    if  len(polygons) > 1:
        new_geom = ogr.Geometry(ogr.wkbMultiPolygon)
        for poly in polygons:
            new_geom.AddGeometry(poly)
    else:
        new_geom = polygons.pop()
    return new_geom

class DatabaseConn(object):
    
    def __init__(self,conf):
        
        self.conf = conf
        self.pg_drv = ogr.GetDriverByName('PostgreSQL')
        if self.pg_drv is None:
            logger.fatal('Could not load the OGR PostgreSQL driver')
            sys.exit(1)
        
        self.pg_uri = 'PG:dbname=' + conf.db_name
        if conf.db_host:
            self.pg_uri = self.pg_uri + ' host=' +  conf.db_host
        if conf.db_port:
            self.pg_uri = self.pg_uri + ' port=' +  conf.db_port
        if conf.db_user:
            self.pg_uri = self.pg_uri + ' user=' +  conf.db_user
        if conf.db_pass:
            self.pg_uri = self.pg_uri + ' password=' +  conf.db_pass

            
        self.pg_ds = None
        
    def connect(self):
        try:
            self.pg_ds = self.pg_drv.Open(self.pg_uri, update = 1)
        except Exception as e:
            logger.fatal("Can't open PG output database: " + str(e))
            sys.exit(1)
         
        if self.conf.db_rolename:
           self.pg_ds.ExecuteSQL("SET ROLE " + self.conf.db_rolename)
                   
class ConfReader(object):
    
    def __init__(self):
        
        usage = "usage: %prog config_file.ini"
        parser = OptionParser(usage=usage)
        (cmd_opt, args) = parser.parse_args()
           
        if len(args) == 1:
            config_files = [args[0]]
        else:
            config_files = ['download_admin_bdys.ini']
        
        parser = SafeConfigParser()
        found = parser.read(config_files)
        if not found:
            sys.exit('Could not load config ' + config_files[0] )
        
        # set up logging
        logging.config.fileConfig(config_files[0], defaults={ 'hostname': socket.gethostname() })
        logger = logging.getLogger()
        
        self.db_host = None
        self.db_rolename = None
        self.db_port = None
        self.db_user = None
        self.db_pass = None
        self.db_schema = 'public'
        self.layer_name = None
        self.layer_geom_column = None
        self.layer_output_srid = 4167
        self.create_grid = False
        self.grid_res = 0.05
        self.shift_geometry = False
        
        self.base_uri = parser.get('source', 'base_uri')
        self.db_name = parser.get('database', 'name')
        self.db_schema = parser.get('database', 'schema')
        
        if parser.has_option('database', 'rolename'):
            self.db_rolename = parser.get('database', 'rolename')
        if parser.has_option('database', 'host'):
            self.db_host = parser.get('database', 'host')
        if parser.has_option('database', 'port'):
            self.db_port = parser.get('database', 'port')
        if parser.has_option('database', 'user'):
            self.db_user = parser.get('database', 'user')
        if parser.has_option('database', 'password'):
            self.db_pass = parser.get('database', 'password')
            
        self.layer_name = parser.get('layer', 'name')
        self.layer_geom_column = parser.get('layer', 'geom_column')
        if parser.has_option('layer', 'output_srid'):
            self.layer_output_srid = parser.getint('layer', 'output_srid')
        if parser.has_option('layer', 'create_grid'):
            self.create_grid = parser.getboolean('layer', 'create_grid')
        if parser.has_option('layer', 'grid_res'):
            self.grid_res = parser.getfloat('layer', 'grid_res')
        if parser.has_option('layer', 'shift_geometry'):
            self.shift_geometry = parser.getboolean('layer', 'shift_geometry')
            
        #meshblocks
        for section in ('meshblock','nzlocalities'):
            for option in parser.options(section): 
                setattr(self,'{}_{}'.format(section,option),parser.get(section,option))
            
        # set up logging
        global logger
        logging.config.fileConfig(config_files[0], defaults={ 'hostname': socket.gethostname() })
        logger = logging.getLogger()
    
        logger.info('Starting download TA boundaries')
    


class meshblocks(object):
    
    f2t = {'Stats_MB_WKT.csv':'meshblock', 'Stats_Meshblock_concordance_WKT.csv':'meshblock_concordance', 'Stats_TA_WKT.csv':'territorial_authority'}
    enc = 'utf-8-sig'
    
    def __init__(self,conf,db): 
        self.db = db
        self.conf = conf
        self.insert(self.fetch())
        
    def fetch(self):
        
        transport = Transport((self.conf.meshblock_ftphost,int(self.conf.meshblock_ftpport)))
        transport.connect(hostkey=None, username=self.conf.meshblock_ftpuser, password=self.conf.meshblock_ftppass, pkey=None)
                    #gss_host=None, gss_auth=False, gss_kex=False, gss_deleg_creds=True)# not in this version
        sftp = SFTPClient.from_transport(transport) 
        for fname in sftp.listdir('.'):
            fmatch = re.match(self.conf.meshblock_ftpregex,fname)
            if fmatch: 
                localfile = fmatch.group(0)
                break
        if not localfile: sys.exit(1)
        
        localpath = '{}/{}'.format(self.conf.meshblock_localpath,localfile)
        remotepath = '{}/{}'.format(self.conf.meshblock_ftppath,localfile)
        print (remotepath,'->',localpath)
        sftp.get(remotepath, localpath)
                
        sftp.close()
        transport.close()
        return localpath
        
    def insert(self,mb):
        self.db.connect()
        #mb = '/home/jramsay/Downloads/Stats_MB_TA_WKT_20160415-NEW.zip'
        with ZipFile(mb,'r') as h:
            for fname in h.namelist():
                first = True
                with h.open(fname,'r') as fh:
                    for line in fh:
                        line = line.strip().decode(self.enc)#.replace('"','\'')
                        if first: 
                            headers = line.split(',')
                            first = False
                        else:
                            values = line.replace("'","''").split(',',len(headers)-1)
                            #if int(values[0])<47800:continue
                            if '"NULL"' in values: continue
                            qry = self.query(self.conf.db_schema,self.f2t[fname],headers,values)
                            try:
                                self.db.pg_ds.ExecuteSQL(qry)
                            except Exception as e:
                                logger.error('Error inserting MB data into {}\n{}'.format(fname,e))
                                
                        
    def query(self,schema,table,headers,values):
        q = 'INSERT INTO {}.{} ({}) VALUES ({})'.format(schema,table,','.join(headers).lower(),','.join(values)).replace('"','\'')
        #q = 'INSERT INTO {}.{} VALUES ({})'.format(schema,table,','.join(line)).replace('"','\'')
        return q

    
class nzfslocalities(object):
    
    prefixes = ('shp','shx','dbf','prj')
    
    def __init__(self,conf,db):
        self.conf = conf
        self.db = db
        self.driver = ogr.GetDriverByName('ESRI Shapefile')
        self.shape_ds = None
        layer = self.fetch()
        self.insert(layer)
        self.shape_ds.Destroy()
    
    def fetch(self):
        prefix = 'shp'
        remotepath = '{}{}.{}'.format(self.conf.nzlocalities_filepath,self.conf.nzlocalities_filename,prefix)
        self.shape_ds = self.driver.Open(remotepath,0)
        if self.shape_ds:
            layer = self.shape_ds.GetLayer(0)
            return layer
        
    def insert(self,in_layer):
        self.db.connect()
        
        create_opts = ['GEOMETRY_NAME='+'geom']
        create_opts.append('SCHEMA=' + 'admin_bdys')
        create_opts.append('OVERWRITE=' + 'yes')
        
        try:
            out_name = in_layer.GetName()
            out_srs = in_layer.GetSpatialRef()
            out_layer = self.db.pg_ds.CreateLayer(
                out_name,
                srs = out_srs,
                geom_type = ogr.wkbMultiPolygon,
                options = create_opts
            )
        except Exception as e:
            logger.fatal('Can not create NZ_Localities output table {}'.format(e))
            sys.exit(1)
            
        try:
            in_layer.ResetReading()
            feature = in_layer.GetNextFeature()
            while feature:
                geom = feature.GetGeometryRef()
                #esri fix
                geom = fix_esri_polyon(geom)
                #poly to multi
                if geom.GetGeometryType() == ogr.wkbPolygon:
                    geom = ogr.ForceToMultiPolygon(geom)
                #geog to geom
                if out_srs.IsGeographic() and self.conf.shift_geometry:
                    shift_geom(geom)
                feature.SetGeometry(geom)
                out_layer.CreateFeature(feature)
                feature = in_layer.GetNextFeature()
            
        except Exception as e:
            logger.fatal('Can not populate NZ_Localities output table {}'.format(e))
            sys.exit(1)
        
    
'''
TODO
file name reader, db overwrite
'''
def main():
    
    c = ConfReader()
    d = DatabaseConn(c)
        
    #taboundaries(c)
    meshblocks(c,d)
    nzfslocalities(c,d)
    
if __name__ == "__main__":
    main()
