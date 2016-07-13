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
# Notes
# to fetch localities file need share created to \\prdassfps01\GISData\Electoral specific\Enrollment Services\Meshblock_Address_Report
# to fetch meshblock data need sftp connection to 144.66.244.17/Meshblock_Custodianship 
# without updated python >2.7.9 cant use paramiko (see commit history) use pexpect instead
# database conn uses lds_bde user and modifed pg_hba allowing; local, lds_bde, linz_db, peer 

__version__ = 1.0

import os
import sys
import re
import json
import string
import socket
import urllib2
import logging.config
import getopt

import socket,time
from zipfile import ZipFile

#from paramiko import Transport, SFTPClient

# from twisted.internet import reactor
# from twisted.internet.defer import Deferred
# from twisted.conch.ssh.common import NS
# from twisted.conch.scripts.cftp import ClientOptions
# from twisted.conch.ssh.filetransfer import FileTransferClient
# from twisted.conch.client.connect import connect
# from twisted.conch.client.default import SSHUserAuthClient, verifyHostKey
# from twisted.conch.ssh.connection import SSHConnection
# from twisted.conch.ssh.channel import SSHChannel
# from twisted.python.log import startLogging, err

from subprocess import Popen,PIPE,check_output

import pexpect


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

PREFIX = 'temp_'

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

class ColumnMapperError(Exception):pass
class ColumnMapper(object):
    '''Acions the list of column mappings from conf file'''
    map = {}
    dra = {'drop':'ALTER TABLE {schema}.{table} DROP COLUMN {drop}',
           'rename':'ALTER TABLE {schema}.{table} RENAME COLUMN {old} TO {new}',
           'add':'ALTER TABLE {schema}.{table} ADD COLUMN {add} {type}',
           'cast':'ALTER TABLE {schema}.{table} ALTER COLUMN {cast} SET DATA TYPE {type}'
    }
    
    def __init__(self,conf):
        self.schema = conf.db_schema
        for attr in conf.__dict__:
            m = re.search('(\w+)_colmap',attr)
            if m: self.map[m.group(1)] = json.loads(getattr(conf,attr))

    def action(self,section,tablename,action):
        '''Generate queries from the column map'''
        _test = self.map.has_key(section) and self.map[section].has_key(tablename) and self.map[section][tablename].has_key(action)
        return [self.formqry(action,PREFIX+tablename, sta) for sta in self.map[section][tablename][action]] if _test else []
    
    def _getArgs(self,a):
        return a.values() if type(a) in (dict,) else a
        
    def formqry(self,action,table,args):
        #print action, table, args
        if action == 'drop': return self.dra[action].format(schema=self.schema,table=table,drop=args)
        elif action == 'rename': return self.dra[action].format(schema=self.schema,table=table,old=args['old'],new=args['new'])
        elif action == 'add': return self.dra[action].format(schema=self.schema,table=table,add=args['add'],type=args['type'])
        elif action == 'cast': return self.dra[action].format(schema=self.schema,table=table,cast=args['cast'],type=args['type'])
        raise ColumnMapperError('Unrecognised query type specifier, use drop/add/rename/cast')    
    
    def _formqry(self,f,d):
        print f,d
        print f.format(*d)
        return f.format(*d)
        
    
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
        if conf.db_schema:
            pass
            #self.pg_uri = self.pg_uri + ' active_schema=' +  conf.db_schema
        
        self.pg_ds = None
        
    def connect(self):
        try:
            self.pg_ds = self.pg_drv.Open(self.pg_uri, update = 1)
        except Exception as e:
            logger.fatal("Can't open PG output database: " + str(e))
            sys.exit(1)
         
        if self.conf.db_rolename:
           self.pg_ds.ExecuteSQL("SET ROLE " + self.conf.db_rolename)
           
    def disconnect(self):
        del self.pg_ds
                   
class ConfReader(object):
    
    def __init__(self):
        
        usage = "usage: %prog config_file.ini"
        parser = OptionParser(usage=usage)
        (cmd_opt, args) = parser.parse_args()
           
        #if len(args) == 1:
        #    config_files = [args[0]]
        #else:
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
        for section in ('connection','meshblock','nzlocalities'):
            for option in parser.options(section): 
                setattr(self,'{}_{}'.format(section,option),parser.get(section,option))
            
        # set up logging
        global logger
        logging.config.fileConfig(config_files[0], defaults={ 'hostname': socket.gethostname() })
        logger = logging.getLogger()
    
        logger.info('Starting download TA boundaries')
    
class ProcessorException(Exception):pass
class Processor(object):
    mbcc = ('OBJECTID','Meshblock','TA','TA Ward','Community Board','TA Subdivision','TA Maori_Ward','Region', \
            'Region Constituency','Region Maori Constituency','DHB','DHB Constituency','GED 2007','MED 2007', \
            'High Court','District Court','GED','MED','Licensing Trust Ward')
    #filename to table+column name translations
    f2t = {'Stats_MB_WKT.csv':['meshblock','<todo create columns>'], \
           'Stats_Meshblock_concordance.csv':['meshblock_concordance',mbcc], \
           'Stats_Meshblock_concordance_WKT.csv':['meshblock_concordance',mbcc], \
           'Stats_TA_WKT.csv':['territorial_authority','<todo create columns>']}
           
    l2t = {'nz_localities':['nz_locality','<todo create columns>'],
           'StatsNZ_Meshblock':['statsnz_meshblock','<todo create columns>'],
           'StatsNZ_TA':['statsnz_ta','<todo create columns>']}
    
    q = {'find':"select count(*) from information_schema.tables where table_schema like '{}' and table_name = '{}'",
         'create':'create table {}.{} ({})',
         'insert':'insert into {}.{} ({}) values ({})',
         'trunc':'truncate table {}.{}',
         'drop':'drop table if exists {}.{}'}
    
    enc = 'utf-8-sig'
    
    def __init__(self,conf,db,cm,sf):
        self.conf = conf
        self.db = db
        self.cm = cm
        self.driver = ogr.GetDriverByName('ESRI Shapefile')
        self.sftp = sf
        self.secname = type(self).__name__.lower()

    def extract(self,file):
        '''Takes a zip path/filename input and returns the path/names of any unzipped files'''
        nl = []
        with ZipFile(file,'r') as h:
            nl = h.namelist()
            for fname in nl:
                h.extract(fname)
        return ['{}/{}'.format(getattr(self.conf,'{}_localpath'.format(self.secname)),n) for n in nl] 
    
    @classmethod
    def recent(cls,filelist,pattern='[a-zA-Z_]*(\d{8}).*'):
        '''get the latest date labelled file from a list'''
        extract = {re.match(pattern,val).group(1):val for val in filelist if re.match(pattern,val)} 
        return extract[max(extract)]
        
    def delete(self,file):
        '''clean up unzipped shapefile'''
        #for file in path:
        p,f = os.path.split(file)
        ff,fx = os.path.splitext(f)
        for candidate in os.listdir(p):
            cf,cx = os.path.splitext(candidate)
            if re.match(ff,cf): os.remove(os.path.join(p,candidate))
            
    def query(self,schema,table,headers='',values='',op='insert'):
        h = ','.join([i.replace(' ','_') for i in headers]).lower() if hasattr(headers,'__iter__') else headers
        v = ','.join(values) if hasattr(values,'__iter__') else values
        return self.q[op].format(schema,table,h, v).replace('"','\'')
    
    def layername(self,in_layer):
        '''Returns the name of the layer that inserting a shapefile would create'''
        in_name = in_layer.GetName()
        return self.l2t[in_name][0] if self.l2t.has_key(in_name) else in_name
        
    def deletelyr(self,tname):
        #dlayer = self.db.pg_ds.GetLayerByName('{}.{}'.format(self.conf.db_schema,tname))
        #self.db.pg_ds.DeleteLayer(dlayer.GetName())
        self.db.pg_ds.DeleteLayer('{}.{}{}'.format(self.conf.db_schema,PREFIX,tname))
        
    def insertshp(self,in_layer):
        if not in_layer: raise ProcessorException('Attempt to process Empty Datasource')
        in_name,out_name = None,None

        #options
        create_opts = ['GEOMETRY_NAME='+'geom']
        create_opts.append('SCHEMA=' + self.conf.db_schema)
        create_opts.append('OVERWRITE=' + 'yes')
        
        #create new layer
        try: 
            in_name = in_layer.GetName()
            logger.info('Inserting shapefile {}'.format(in_name))
            out_name = PREFIX+self.l2t[in_name][0] if self.l2t.has_key(in_name) else in_name

            out_srs = in_layer.GetSpatialRef()
            out_layer = self.db.pg_ds.CreateLayer(
                out_name,
                srs = out_srs,
                geom_type = ogr.wkbMultiPolygon,
                options = create_opts
            )
            #build layer fields
            in_ldef = in_layer.GetLayerDefn()
            for i in range(0, in_ldef.GetFieldCount()):
                in_fdef = in_ldef.GetFieldDefn(i)
                out_layer.CreateField(in_fdef)
                
        except Exception as e:
            logger.fatal('Can not create {} output table {}'.format(out_name,e))
            sys.exit(1)
            
        #insert features
        try:
            in_layer.ResetReading()
            in_feat = in_layer.GetNextFeature()
            out_ldef = out_layer.GetLayerDefn()
            while in_feat:                
                out_feat = ogr.Feature(out_ldef)
                for i in range(0, out_ldef.GetFieldCount()):
                    out_feat.SetField(out_ldef.GetFieldDefn(i).GetNameRef(), in_feat.GetField(i))
                geom = in_feat.GetGeometryRef()
                #1. fix_esri_polygon (no longer needed?)
                #geom = fix_esri_polyon(geom)
                #2. shift_geom
                if out_srs.IsGeographic() and self.conf.shift_geometry:
                        shift_geom(geom)
                #3. always force, bugfix
                geom = ogr.ForceToMultiPolygon(geom)
                out_feat.SetGeometry(geom)
                out_layer.CreateFeature(out_feat)
                in_feat = in_layer.GetNextFeature()
            
        except Exception as e:
            logger.fatal('Can not populate {} output table {}'.format(e))
            sys.exit(1)
            
        return out_name
            
    def insertcsv(self,mbfile):
        fp,ff = os.path.split(mbfile) 
        logger.info('Inserting csv {}'.format(ff))
        self.db.connect()
        #mb = '/home/jramsay/Downloads/Stats_MB_TA_WKT_20160415-NEW.zip'
        first = True
        # this is a hack while using temptables
        csvhead = self.f2t[ff]
        csvhead[0] = PREFIX+csvhead[0]
        with open(ff,'r') as fh:
            for line in fh:
                line = line.strip().decode(self.enc)#.replace('"','\'')
                if first: 
                    headers = line.split(',')
                    findqry = self.query(self.conf.db_schema,csvhead[0],op='find')
                    if self.execute(findqry).GetNextFeature().GetFieldAsInteger(0) == 0:
                        storedheaders = ','.join(['{} VARCHAR'.format(m.replace(' ','_')) for m in csvhead[1]])
                        createqry = self.query(self.conf.db_schema,csvhead[0],storedheaders,op='create')
                        self.execute(createqry)
                    else:
                        truncqry = self.query(self.conf.db_schema,csvhead[0],op='trunc')
                        self.execute(truncqry)
                    first = False
                else:
                    values = line.replace("'","''").split(',',len(headers)-1)
                    #if int(values[0])<47800:continue
                    if '"NULL"' in values: continue
                    insertqry = self.query(self.conf.db_schema,csvhead[0],headers,values,op='insert')
                    self.execute(insertqry)
                    
        return csvhead[0]
                           
    def mapcolumns(self,tablename):
        '''Perform input to final column mapping'''
        for qlist in [self.cm.action(self.secname,tablename.lower(),adrc) for adrc in ('add','drop','rename','cast')]: 
            for q in qlist: 
                if q: self.execute(q)
                
    def drop(self,table):
        '''Clean up any previous table instances. Doesn't work!''' 
        return self.execute(self.q['drop'].format(self.conf.db_schema,table))
                              
    def execute(self,q):  
        try:
            logger.info('Executing SQL {}'.format(q))
            return self.db.pg_ds.ExecuteSQL(q)
        except Exception as e:
            logger.error('Error executing query {}\n{}'.format(q,e))
    
class Meshblock(Processor):
    '''Extract and process the meshblock, concordance and boundaries layers'''
    
    def __init__(self,conf,db,cm,sf):
        super(Meshblock,self).__init__(conf,db,cm,sf)   
             
    def run(self):
        self.get()
        return self.process()
        
    def get(self): 
        dfile = self.sftp.fetch(self.secname)
        #dfile='./Stats_Meshblock_concordance_20160607.zip'
        if re.search('\.zip$',dfile): 
            self.file = self.extract(dfile)
        else: self.file = [dfile,]
        
    def process(self,pathlist=None):
        tlist = ()
        self.db.connect()
        ds = None
        if not pathlist: pathlist = [f for f in self.file if re.search('\.csv$|\.shp$',f)]
        #extract the shapefiles
        for mbfile in pathlist:
            #extract the shapefiles
            if re.match('.*\.shp$',mbfile):
                #self.mapcolumns(type(self).__name__.lower(),self.insertshp(self.driver.Open(mbfile,0).GetLayer(0))) #Gives OGR error!!! Assume unreferenced DS is GC'd?
                mbhandle = self.driver.Open(mbfile,0)
                mblayer = mbhandle.GetLayer(0)
                tname = self.layername(mblayer)
                #self.drop(tname) #this doesn't work for some reason
                self.deletelyr(tname)
                self.insertshp(mblayer)
                self.mapcolumns(tname)
                tlist += tname
                mbhandle.Destroy()
                
            #extract the concordance csv
            elif re.match('.*\.csv$',mbfile):
                tname = self.insertcsv(mbfile)
                self.mapcolumns(tname)
                tlist += (tname,)
            
            self.delete(mbfile)
        self.db.disconnect()
        return tlist

        
class NZLocalities(Processor):
    '''Exract and process the nz_localities file'''
    #NB new format, see nz_locality
    
    def __init__(self,conf,db,cm,sf):
        super(NZLocalities,self).__init__(conf,db,cm,sf)
        
    def run(self):
        self.get()
        return self.process()
        
    def get(self): 
        pass
    
    def process(self,pathlist=None):
        tlist = ()
        self.db.connect()
        ds = None
        if not pathlist: pathlist = '{}{}.shp'.format(self.conf.nzlocalities_filepath,self.conf.nzlocalities_filename)
        ds = self.driver.Open(pathlist,0)
        if ds:
            #self.mapcolumns(type(self).__name__.lower(),self.insertshp(ds.GetLayer(0)))
            tname = 'nz_locality'
            self.mapcolumns('nz_locality')
            tlist += (tname,)
            ds.Destroy()
        self.db.disconnect()
        return tlist
        
class Version(object):
    
    importfile = 'aimsref_import.sql'
    qtv = 'select table_version.ver_apply_table_differences({}, {}, {})'
    
    def __init__(self,conf,db):
        self.conf = conf
        self.db = db
        
    def setup(self):
        '''Create temp schema'''
        tv = None
        self.db.connect()
        #self.db.pg_ds.ExecuteSQL('CREATE SCHEMA temp_{}'.format(self.conf.db_schema))
        with open(self.importfile,'r') as h:
            tv = h.read()
#         use this to check for foreign characters
#         n=0
#         for l in tv:
#             print n,l
#             n+=1
        self.db.pg_ds.ExecuteSQL(tv)
        self.db.disconnect()
        
        
    def versiontables(self,tablelist):
        for t in tablelist:
            self.db.pg_ds.ExecuteSQL('SELECT {}.import_admin_boundary({})'.format(self.conf.db_schema,t))
            #self.db.pg_ds.ExecuteSQL('SELECT {}.import_admin_boundary({})'.format(self.conf.db_schema,t))
            
    def teardown(self):
        '''drop temp schema'''
        self.db.pg_ds.ExecuteSQL('DROP SCHEMA IF EXISTS temp_{}'.format(self.conf.db_schema))
        
    
class PExpectException(Exception):pass
class PExpectSFTP(object):  
      
    def __init__(self,conf):
        self.conf = conf
        self.target = '{}@{}:{}'.format(self.conf.connection_ftpuser,self.conf.connection_ftphost,self.conf.connection_ftppath)
        self.opts = ['-o','PasswordAuthentication=yes',self.target]
        
    def fetch(self,dfile):
        pattern = getattr(self.conf,'{}_filepattern'.format(dfile))
        localpath,localfile = None,None
        filelist = []
        prompt = 'sftp> '
        get_timeout = 60.0
        sftp = pexpect.spawn('sftp',self.opts)
        try:
            if sftp.expect('(?i)password:')==0:
                sftp.sendline(self.conf.connection_ftppass)
                if sftp.expect(prompt) == 0:
                    sftp.sendline('ls')
                    if sftp.expect(prompt) == 0:
                        for fname in sftp.before.split()[1:]:
                            fmatch = re.match(pattern,fname)
                            if fmatch: filelist += [fname,]
                        fname = Processor.recent(filelist,pattern)
                        localfile = re.match(pattern,fname).group(0)
                        #break
                        if not localfile: 
                            raise PExpectException('Cannot find matching file pattern')
                    else:
                        raise PExpectException('Unable to access or empty directory at {}'.format(self.conf.connection_ftppath))
                    localpath = '{}/{}'.format(getattr(self.conf,'{}_localpath'.format(dfile)),localfile)
                    sftp.sendline('get {}'.format(localfile))
                    if sftp.expect(prompt,get_timeout) != 0:
                        raise PExpectException('Cannot retrieve file, {}/{}'.format(self.conf.connection_ftppath,localfile))
                    os.rename('./{}'.format(localfile),localpath)
                else: 
                    raise PExpectException('Password authentication failed')  
            else:
                raise PExpectException('Cannot initiate session using {}'.format(selt.opts))  
                
        except pexpect.EOF:
            raise PExpectException('End-Of-File received attempting connect')  
        except pexpect.TIMEOUT:
            raise PExpectException('Connection timeout occurred')  
        finally:
            sftp.sendline('bye')
            sftp.close()
            
        return localpath

    
'''
TODO
file name reader, db overwrite
'''
def main():   
    t = () 
    try:
        opts, args = getopt.getopt(sys.argv[1:], "h", ["help"])
    except getopt.error, msg:
        print msg
        print "for help use --help"
        sys.exit(2)
        
        
    for opt, val in opts:
        if opt in ("-h", "--help"):
            print __doc__
            sys.exit(0)
        elif opt in ("-v", "--version"):
            print __version__
            sys.exit(0)

    c = ConfReader()
    m = ColumnMapper(c)
    d = DatabaseConn(c)
    s = PExpectSFTP(c)
    v = Version(c,d)
    
    v.setup()
    
    #is territorial_authority included in the meshblocks download the same as the old taboundaries    
    #if len(args)==0 or 't' in args:
    #    taboundaries(c)    
    if len(args)==0 or 'm' in args:
        mbk = Meshblock(c,d,m,s)
        t += (mbk.run(),)
    if len(args)==0 or 'l' in args:
        nzl = NZLocalities(c,d,m,s) 
        t += (nzl.run(),)
        
    v = Version(d)
    v.versiontables(t)
    v.teardown()
    
if __name__ == "__main__":
    main()
    print 'finished'



