# Note about spatial selections with PostGIS

`EXPLAIN SELECT` - tells you what is being selected.

```
EXPLAIN SELECT * FROM lite_2_0.observations_1999_land_0 WHERE observed_variable IN (44,85) AND data_policy_licence = 1 AND quality_flag = 0 AND ST_Polygon('LINESTRING(-1.0 55.0, -1.0 59.0, 3.0 59.0, 3.0 55.0, -1.0 55.0)'::geometry, 4326) && location AND ST_Intersects(ST_Polygon('LINESTRING(-1.0 55.0, -1.0 59.0, 3.0 59.0, 3.0 55.0, -1.0 55.0)'::geometry, 4326), location) AND date_trunc('hour', date_time) = TIMESTAMP '1999-03-03 06:00:00';

 - shows:
  - indexes not being used: 
    - "observations_1999_land_0_location_idx" btree (location)
    - "observations_1999_land_0_date_time_idx" btree (date_time) CLUSTER
    
result of EXPLAIN:
         Filter: (('0103000020E61000000100000005000000000000000000F0BF0000000000804B40000000000000F
0BF0000000000804D4000000000000008400000000000804D4000000000000008400000000000804B40000000000000F0BF
0000000000804B40'::geography && location) AND ('0103000020E61000000100000005000000000000000000F0BF0
000000000804B40000000000000F0BF0000000000804D4000000000000008400000000000804D4000000000000008400000
000000804B40000000000000F0BF0000000000804B40'::geography && location) AND (date_time > '1999-03-03
06:00:00+00'::timestamp with time zone) AND (data_policy_licence = 1) AND (quality_flag = 0) AND (s
t_distance('0103000020E61000000100000005000000000000000000F0BF0000000000804B40000000000000F0BF00000
00000804D4000000000000008400000000000804D4000000000000008400000000000804B40000000000000F0BF00000000
00804B40'::geography, location, false) < '1e-05'::double precision))
         ->  Bitmap Index Scan on observations_1999_land_0_observed_variable_idx  (cost=0.00..15218
5.83 rows=8718495 width=0)
               Index Cond: (observed_variable = ANY ('{44,85}'::integer[]))
```

CHANGED:

 1. Change location index from "btree" to "gist"
 Tried:
    CREATE INDEX observations_1919_land_0_location_gist_idx ON lite_2_0.observations_1919_land_0 USING gist ( location );
    CREATE INDEX observations_1998_land_0_location_gist_idx ON lite_2_0.observations_1998_land_0 USING gist ( location );
    - takes 10 minutes to create INDEX
    CREATE INDEX observations_1999_land_0_location_gist_idx ON lite_2_0.observations_1999_land_0 USING gist ( location );

 Can undo with: 
    DROP INDEX lite_2_0.observations_1919_land_0_location_gist_idx ;
 2. See changes below
```
EXPLAIN SELECT
                COUNT(*)
FROM
                lite_2_0.observations_1919_land_0
WHERE
                location && ST_GeogFromText('SRID=4326;POLYGON((-179.0 -55.0, -179.0 59.0, 3.0 59.0, 3.0 -55.0, -179.0 -55.0))')
                AND ST_Intersects(ST_GeogFromText('SRID=4326;POLYGON((-179.0 -55.0, -179.0 59.0, 3.0 59.0, 3.0 -55.0, -179.0 -55.0))'), location)
                AND date_time > timestamp with time zone '1919-03-03 06:00:00';
```
 - does work - says the spatial index is being used.
 - the datetime index is not being used
   - BUT: I think the clustering on that field (which has been done) overrides use of an index.

 
## Use of "&&" Operator 

--- 

If I was to write this query it would look something like

```
SELECT 
                * 
FROM 
                lite_2_0.observations_1999_land_0 
WHERE 
                observed_variable IN (44,85) 
                AND data_policy_licence = 1 
                AND quality_flag = 0 
                AND location && ST_GeogFromText('SRID=4326;POLYGON((-1.0 55.0, -1.0 59.0, 3.0 59.0, 3.0 55.0, -1.0 55.0))')
                AND ST_Intersects(ST_GeogFromText('SRID=4326;POLYGON((-1.0 55.0, -1.0 59.0, 3.0 59.0, 3.0 55.0, -1.0 55.0))'), location)
                AND date_time > timestamp with time zone '1999-03-03 06:00:00';           
```

Changed to use ST_GeogFromText to explicitly cast as the geography rather than geometry data type
(location was a geography type but you may have made it geometry in the cdm lite). This is probably
the same as the LINESTRING and ST_POLYGON but I’m not too familiar with these commands.

Made it so the date / time comparison is explicitly the same as the date_time column. Again it would
be worth checking to make sure that they are still in agreement with the cdm lite_2_0.

In terms of `&&` and `ST_Intersects` – they do different things and will return different results.
&& checks the bounding boxes, using an index if defined, some results from outside of the polygon
may be returned. ST_Interescts then does a more accurate check so we probably want to use both, one
to get all potential hits quickly and then the other to do the accurate calculate.

 
