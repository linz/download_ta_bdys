#LINZ Download TA Boundaries

This LINZ software provides a script to download and update [Statistics New Zealand](http://www.stats.govt.nz/) Territorial Authority (TA) boundaries from the [Stats ArcGIS REST API service](http://maps.stats.govt.nz/wss/service/arcgis1/guest/) to a PostgreSQL/PostGIS Database. It optionally translates the downloads geometries to a coordinate system of your choice, translates the longitudes to 0-360 range, and builds a grid index which is suitable for fast intersect relationship tests.

## Configuration setup
Download the code, create a config file or customise the included download_ta_bdy.ini config file. The following configuration options are important to setup:

1. Set up the Stats NZ server URL

    ~~~ ini
    [source]
    base_uri = http://maps.stats.govt.nz/wss/service/arcgis1/guest/Boundaries/Local_government/MapServer
    ~~~

2. Specify how many features to retrieve per request. If it times out or you get errors try a smaller number

    ~~~ ini
    [source]
    page_size = 10
    ~~~ 

3. In the database section of the config file set the output PostgreSQL database name.
  
    ~~~ ini
    [database]
    name = bde_db
    ~~~
    
4. Set the output schema name and any required connection credentials. If not schema name is set then the 'public' schema will be used
   
    ~~~ ini
    name = bde_db
    host = localhost
    user = foo
    password = bar
    schema = my_schema
    ~~~
    
5. Set the output TA layer name, output coordinate system ([EPSG id](http://spatialreference.org)) and if you want to shift longitudes to 0-360
    
    ~~~ ini
    [layer]
    name = territorial_authority
    output_srid = 4167
    shift_geometry = True
    ~~~
    
6. Optionally create a grid index once the TA boundary layer has been created. grid_res is X & Y resolution (in coordinate system units) of the grid to be created

    ~~~ ini
    [layer]
    grid_res = 0.05
    shift_geometry = True
    ~~~

7. Optionally set it to update the data every time regardless of whether Stat has released a new version. Useful for testing

    ~~~ ini
    [layer]
    force_update = True
    ~~~
    
8. Setup the logging parameters. The most important setting is to setup the fileHandler file logging path

    ~~~ ini
    [handler_fileHandler]
    class=FileHandler
    level=DEBUG
    formatter=simpleFormatter
    args=('/path/to/mylogfile.log', 'w')
    ~~~
    
9. Also if you want to email errors from the script to an email account then emailHandler needs to be configured

    ~~~ ini
    [handler_emailHandler]
    class=handlers.SMTPHandler
    level=WARN
    formatter=simpleFormatter
    args=('smpthostname', 'from@abc', ['user1@abc', 'user2@xyz'], 'Email Subject')
    ~~~
    
    You will also need to ensure logger key uses the email handler:
    ~~~ ini
    [loggers]
    keys=root,email
    ~~~

## Requirements

- [Python 2.7+][^python]
- [Python bindings of GDAL 1.10+][^gdal]

[^gdal]: http://www.gdal.org
[^python]: http://www.python.org

## License
This project is under 3-clause BSD License, except where otherwise specified. See the LICENSE file for more details.
