Import cvs file into Postgres
=============================================


## csv Processor file
------

DATA,GEO,VALOR
dm,dm,ft
dm_date,dm_geographyloures,ft_corine
year,freguesia_code,artificial_surface
id,gid,id
data_id,geographyloures_id,none


## json Processor file
------

{
   "name" : "eenvplus",
   "locale":"en",
   "processor" : [{"tag" : "DATA", 
                   "type" : "dm", 
                   "table" : "dm_date", 
                   "value" : "year", 
                   "pkey" : "id",
                   "fkey" : "data_id"},
                  {"tag" : "GEO", 
                   "type" : "dm", 
                   "table" : "dm_geographyloures", 
                   "value" : "freguesia_code",
                   "pkey" : "gid",
                   "fkey" : "geographyloures_id"},
                  {"tag" : "VALOR", 
                   "type" : "ft", 
                   "table" : "ft_corine", 
                   "value" : "artificial_surface",
                   "pkey" : "id",
                   "fkey" : "none"}],
   "cubes": [
        {
        ............................................

## Data file tested
------

DATA,GEO,VALOR
2013,110702,35
2012,110702,35
2011,110702,35
2010,110702,35
2009,110702,35
2008,110702,35
2013,110703,38
2012,110703,5.3
2011,110703,5.3
2010,110703,5.3
2009,110703,5.3
2008,110703,5.3
2013,110704,80
2012,110704,80
2011,110704,80
2010,110704,80
2009,110704,80
2008,110704,135.5