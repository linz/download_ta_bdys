################################################################################
#
# download_ta_bdys.py
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

# require version 1.11.2 or higher to resolve https://trac.osgeo.org/gdal/ticket/5538
if version_num < 1110200:
    sys.exit('ERROR: Python bindings of GDAL 1.11.2 or later required')

# if GDAL 2.0 or higher then use retry option for HTTP 500 timeout issues
if version_num >= 2000000:
    gdal.SetConfigOption('GDAL_HTTP_RETRY_DELAY', '10')
    gdal.SetConfigOption('GDAL_HTTP_MAX_RETRY', '5')

# make sure gdal exceptions are not silent
gdal.UseExceptions()
osr.UseExceptions()
ogr.UseExceptions()

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

def main():
    
    usage = "usage: %prog config_file.ini"
    parser = OptionParser(usage=usage)
    (cmd_opt, args) = parser.parse_args()
       
    if len(args) == 1:
        config_files = [args[0]]
    else:
        config_files = ['download_ta_bdys.ini']
    
    parser = SafeConfigParser()
    found = parser.read(config_files)
    if not found:
        sys.exit('Could not load config ' + config_files[0] )
    
    # set up logging
    logging.config.fileConfig(config_files[0], defaults={ 'hostname': socket.gethostname() })
    logger = logging.getLogger()

    logger.info('Starting download TA boundaries')
    db_host = None
    db_rolename = None
    db_port = None
    db_user = None
    db_pass = None
    db_schema = 'public'
    layer_name = None
    layer_geom_column = None
    layer_output_srid = 4167
    create_grid = False
    grid_res = 0.05
    shift_geometry = False
    
    base_uri = parser.get('source', 'base_uri')
    db_name = parser.get('database', 'name')
    db_schema = parser.get('database', 'schema')
    
    if parser.has_option('database', 'rolename'):
        db_rolename = parser.get('database', 'rolename')
    if parser.has_option('database', 'host'):
        db_host = parser.get('database', 'host')
    if parser.has_option('database', 'port'):
        db_port = parser.get('database', 'port')
    if parser.has_option('database', 'user'):
        db_user = parser.get('database', 'user')
    if parser.has_option('database', 'password'):
        db_pass = parser.get('database', 'password')
        
    layer_name = parser.get('layer', 'name')
    layer_geom_column = parser.get('layer', 'geom_column')
    if parser.has_option('layer', 'output_srid'):
        layer_output_srid = parser.getint('layer', 'output_srid')
    if parser.has_option('layer', 'create_grid'):
        create_grid = parser.getboolean('layer', 'create_grid')
    if parser.has_option('layer', 'grid_res'):
        grid_res = parser.getfloat('layer', 'grid_res')
    if parser.has_option('layer', 'shift_geometry'):
        shift_geometry = parser.getboolean('layer', 'shift_geometry')
    
    try:
        output_srs = osr.SpatialReference()
        output_srs.ImportFromEPSG(layer_output_srid)
    except:
        logger.fatal("Output SRID %s is not valid" % (layer_output_srid))
        sys.exit(1)
    
    if create_grid and not grid_res > 0:
        logger.fatal("Grid resolution must be greater than 0")
        sys.exit(1)
        
    #
    # Determine TA layer and its year from REST service
    #
    
    logger.debug(base_uri + '?f=json')
    response = urllib2.urlopen(base_uri + '?f=json')
    capabilities = json.load(response)
    
    latest_layer = None
    latest_year = None
    for layer in capabilities['layers']:
        m = re.match(r'^Territorial\sAuthorities\s(\d{4})$', layer['name'])
        if m:
            if not latest_year or int(m.group(1)) > latest_year:
                latest_year = int(m.group(1))
                latest_layer = str(layer['id'])
    
    if not latest_layer:
        logger.fatal('Could not find the TA layer in ' + base_uri)
        sys.exit(1)
    
    #
    # Connect to the PostgreSQL database
    #
    
    pg_drv = ogr.GetDriverByName('PostgreSQL')
    if pg_drv is None:
        logger.fatal('Could not load the OGR PostgreSQL driver')
        sys.exit(1)
    
    pg_uri = 'PG:dbname=' + db_name
    if db_host:
        pg_uri = pg_uri + ' host=' +  db_host
    if db_port:
        pg_uri = pg_uri + ' port=' +  db_port
    if db_user:
        pg_uri = pg_uri + ' user=' +  db_user
    if db_pass:
        pg_uri = pg_uri + ' password=' +  db_pass
    
    pg_ds = None
    try:
        pg_ds = pg_drv.Open(pg_uri, update = 1)
    except Exception, e:
        logger.fatal("Can't open PG output database: " + str(e))
        sys.exit(1)
    
    if db_rolename:
       pg_ds.ExecuteSQL("SET ROLE " + db_rolename)
    
    #
    # Check the current database TA table year. Only continue if data is old.
    #
    
    output_lyr = None
    full_layer_name = db_schema + '.' + layer_name
    try:
        output_lyr = pg_ds.GetLayerByName(full_layer_name)
    except:
        logger.debug(full_layer_name + ' does not exist')
    
    if output_lyr:
        sql = """SELECT
                    description
                 FROM
                    pg_description
                    JOIN pg_class ON pg_description.objoid = pg_class.oid
                    JOIN pg_namespace ON pg_class.relnamespace = pg_namespace.oid
                 WHERE
                    nspname='%s' and
                    relname = '%s' """ % (db_schema, layer_name)
        
        sql_lyr = pg_ds.ExecuteSQL(sql)
        feat = sql_lyr.GetNextFeature()
        if feat:
            current_version = int(feat.GetFieldAsString('description'))
            if current_version <= latest_year:
                logger.info("TA layer does not need to be updated (current version " + \
                    str(current_version) + ")")
                sys.exit(0)
        pg_ds.ReleaseResultSet(sql_lyr)
        
        # truncate data
        pg_ds.ExecuteSQL("TRUNCATE " + full_layer_name)
        
    #
    # Create database table if it doesn't already exist.
    #
    
    if not output_lyr:
        create_opts = ['GEOMETRY_NAME='+layer_geom_column]
        if db_schema:
            create_opts.append('SCHEMA=' + db_schema)
        
        try:
            output_lyr = pg_ds.CreateLayer(
                full_layer_name,
                srs = output_srs,
                geom_type = ogr.wkbMultiPolygon,
                options = create_opts
            )
            name_field = ogr.FieldDefn('name', ogr.OFTString)
            name_field.SetWidth(100)
            output_lyr.CreateField(name_field)
            
            pg_ds.ExecuteSQL("GRANT SELECT ON TABLE " + full_layer_name + " TO public")
        except Exception, e:
            logger.fatal('Can not create TA output table: %s' % (str(e)))
            sys.exit(1)
    
    #
    # Retrieve all features from the ArcGIS endpoint
    #

    layer_output_srs = osr.SpatialReference()
    layer_output_srs.ImportFromEPSG(layer_output_srid)
    geojson_drv = ogr.GetDriverByName('GeoJSON')

    if geojson_drv is None:
        logger.fatal('Could not load the OGR GeoJSON driver')
        sys.exit(1)

    # OGR < 2.0 doesn't support ArcGIS paging
    if version_num < 2000000:

        # Temp layer to store features
        tmp_drv = ogr.GetDriverByName("MEMORY")
        tmp_ds = tmp_drv.CreateDataSource('memDs')
        tmp_opn = tmp_drv.Open('memDs',1)
        input_lyr = tmp_ds.CreateLayer('tmpLyr',srs=layer_output_srs,geom_type=ogr.wkbMultiPolygon)

        # Get a list of Object IDs
        object_ids_uri = base_uri + '/' + str(latest_layer) + \
                '/query?f=json&where=1=1&returnGeometry=False&outFields=objectid'

        try:
            response = urllib2.urlopen(object_ids_uri)
        except Exception, e:
            logger.fatal('Could open uri %s: %s' % (object_ids_uri, str(e)))
            sys.exit(1)

        try:
            json_ids = json.load(response)
        except Exception, e:
            logger.fatal('Could load json from uri %s: %s' % (object_ids_uri, str(e)))
            sys.exit(1)

        geojs, tmp_feat_defn, feature, field_defn = None, None, None, None

        # Retrieve each feature one by one
        for feature in json_ids['features']:

            object_id = feature['attributes']['OBJECTID']

            feature_uri = base_uri + '/' + str(latest_layer) + \
                '/query?f=json&returnGeometry=true&outSR=' + str(layer_output_srid) \
                + '&objectIds=' + str(object_id)

            try:
                geojs = geojson_drv.Open(feature_uri)
            except Exception, e:
                logger.fatal('Could not read geojson from uri %s: %s' % (feature_uri, str(e)))
                sys.exit(1)

            try:
                feature = geojs.GetLayer(0).GetNextFeature()
            except Exception, e:
                logger.fatal('Could get layer from geojson: %s' % (str(e)))
                sys.exit(1)

            if not tmp_feat_defn:
                tmp_feat_defn = feature.GetDefnRef()
                i = 0
                while (i < tmp_feat_defn.GetFieldCount()):
                   field_defn = tmp_feat_defn.GetFieldDefn(i)
                   input_lyr.CreateField(field_defn)
                   i = i + 1

            input_lyr.CreateFeature(feature)

    else:

        layer_uri = base_uri + '/' + str(latest_layer) + \
             '/query?f=json&where=1=1&returnGeometry=true&outSR=' + str(srid)

        try:
            geojs_ds = geojson_drv.Open(layer_uri)
        except Exception, e:
            logger.fatal('Could not read geojson from uri %s: %s' % (layer_uri, str(e)))
            sys.exit(1)

        try:
            input_lyr = geojs_ds.GetLayer(0)
        except Exception, e:
            logger.fatal('Could get layer from geojson: %s' % (str(e)))
            sys.exit(1)

    if input_lyr.GetFeatureCount() < 1:
        logger.fatal('No features found at URL %s: %s' \
            % (base_uri + '/' + str(latest_layer), str(e)))
        sys.exit(1)

    input_defn = input_lyr.GetLayerDefn()
    p = re.compile('^TA' + str(latest_year) + '.+NAME$', flags = re.UNICODE)
    ta_name_field = None
    for i in range( input_defn.GetFieldCount() ):
        field = input_defn.GetFieldDefn( i )
        field_name = field.GetNameRef()
        if p.search(field_name):
            ta_name_field = field_name
    if not ta_name_field:
        logger.fatal("Can not find TA name field")
        sys.exit(1)

    gdal.SetConfigOption('PG_USE_COPY', 'YES')
    
    #
    # Copy data from REST Service to PostgreSQL database
    #
    
    output_defn = output_lyr.GetLayerDefn()
    input_lyr.ResetReading()
    input_feature = input_lyr.GetNextFeature()
    output_lyr.StartTransaction()
    while input_feature is not None:
        output_feature = ogr.Feature(output_defn)
        output_feature['name'] = input_feature[ta_name_field]
        geom = input_feature.GetGeometryRef()
        if geom.GetGeometryType() == ogr.wkbPolygon:
            geom = ogr.ForceToMultiPolygon(geom)
        if output_srs.IsGeographic() and shift_geometry:
            shift_geom(geom)
        output_feature.SetGeometry(geom)
        output_lyr.CreateFeature(output_feature)
        output_feature.Destroy()
        input_feature = input_lyr.GetNextFeature()
    input_feature = None

    try: # TODO: sometimes get an 'General OGR error' while committing?
        output_lyr.CommitTransaction()
    except Exception, e:
        logger.debug("Ignoring commit error: " + str(e))

    pg_ds.ExecuteSQL("ANALYSE " + full_layer_name)
    pg_ds.ExecuteSQL("COMMENT ON TABLE " + full_layer_name + " IS '" + str(latest_year) + "'")
    
    #
    # Create TA grid index if configured
    #
    
    if create_grid:
        sql = "SELECT create_table_polygon_grid('%s', '%s', '%s', %g, %g) as result" \
              % (db_schema, layer_name, layer_geom_column, grid_res, grid_res)
        logger.debug("Building grid with SQL " + sql)
        try:
            sql_lyr = pg_ds.ExecuteSQL(sql)
            if sql_lyr:
                feat = sql_lyr.GetNextFeature()
                logger.info("Created grid layer: " + feat['result'])
        except Exception, e:
            logger.fatal("Failed to create grid layer: " + str(e))
            sys.exit(1)
    
    logger.info("TA layer has been updated to version " + str(latest_year))
    sys.exit(0)


if __name__ == "__main__":
    main()
