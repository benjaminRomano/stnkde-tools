import argparse
import multiprocessing
import psycopg2
from joblib import Parallel, delayed

def main(host, dbname, user, password, lixel_length, srid):
    connection_string = "host={0} dbname={1} user={2} password={3}".format(host, dbname, user, password)
    conn = psycopg2.connect(connection_string)
    conn.autocommit = True
    cur = conn.cursor()

    print("Generating segment points...")

    cur.execute("""
        CREATE TABLE public.segment_points_%(lixel_length)s (
            id serial NOT NULL,
            geom geometry(Point, %(srid)s),
            CONSTRAINT segment_points_%(lixel_length)s_pkey PRIMARY KEY (id)
        );
        CREATE INDEX segment_points_%(lixel_length)s_spatial_index 
            ON public.segment_points_%(lixel_length)s USING gist (geom);
    """, {"lixel_length": lixel_length, "srid": srid})

    generate_segment_points(cur, lixel_length, srid)

    print("Inserting segment points...")
    insert_segment_points(cur, connection_string, lixel_length)

def insert_segment_point_bucket(connection_string, ids, lixel_length):
    conn = psycopg2.connect(connection_string)
    conn.autocommit = True
    cur = conn.cursor()

    for point_id in ids:
        cur.execute("""SELECT add_segment_point_%(lixel_length)s(%(id)s)""", {"lixel_length": lixel_length, "id": point_id})

    cur.close()
    conn.close()

def insert_segment_points(cur, connection_string, lixel_length):
    cur.execute("""SELECT topology.CopyTopology('network_topo', 'network_topo_%s');""", (lixel_length,))
    cur.execute("""
        CREATE OR REPLACE FUNCTION add_segment_point_%(lixel_length)s(point_id int) RETURNS void AS
        $BODY$
        BEGIN
            BEGIN
                PERFORM topology.TopoGeo_AddPoint('network_topo_%(lixel_length)s', 
                (SELECT geom FROM segment_points_%(lixel_length)s WHERE id = point_id), 5);
            EXCEPTION
                WHEN OTHERS THEN
            END;
        END;
        $BODY$
        LANGUAGE 'plpgsql';
    """, {"lixel_length": lixel_length})

    cur.execute("SELECT id FROM segment_points_%s", (lixel_length,))
    rows = cur.fetchall()

    buckets = [[] for i in range(multiprocessing.cpu_count())]
    for row in rows:
        buckets[row[0] % len(buckets)].append(row[0])

    Parallel(n_jobs=-1)(delayed(insert_segment_point_bucket)(connection_string, buckets[i], lixel_length) for i in range(len(buckets)))

    cur.execute("""DROP TABLE segment_points_%s""", (lixel_length,))

def generate_segment_points(cur, lixel_length, srid):
    cur.execute("""
        CREATE OR REPLACE FUNCTION generate_segment_points_%(lixel_length)s() RETURNS void AS
        $BODY$
        DECLARE
            r geometry(LINESTRING, %(srid)s);
        BEGIN
            FOR r IN SELECT geom FROM network_topo.edge_data
            LOOP
            WITH line AS (SELECT r as geom),
            linemeasure AS
            (SELECT
                ST_AddMeasure(line.geom, 0, ST_Length(line.geom)) AS linem,
                generate_series(0, ST_Length(line.geom)::int, %(lixel_length)s) AS i
            FROM line),
            geometries AS (
                SELECT
                i,
                (ST_Dump(ST_GeometryN(ST_LocateAlong(linem, i), 1))).geom AS geom
                FROM linemeasure)

            INSERT INTO public.segment_points_%(lixel_length)s (geom) SELECT ST_SetSRID(ST_MakePoint(ST_X(geom), ST_Y(geom)), %(srid)s) AS geom FROM geometries;
            END LOOP;
        END;
        $BODY$
        LANGUAGE 'plpgsql';
        SELECT generate_segment_points_%(lixel_length)s()
    """, {"lixel_length": lixel_length, "srid": srid})

def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("-host", required=True, dest="host",
                        help="psql host")
    parser.add_argument("-d", required=True, dest="dbname",
                        help="psql database")
    parser.add_argument("-u", required=True, dest="user",
                        help="psql user")
    parser.add_argument("-p", required=True, dest="password",
                        help="psql password")
    parser.add_argument("-l", type=int, required=True, dest="lixel_length",
                        help="lixel length")
    parser.add_argument("-s", type=int, required=True, dest="srid",
                        help="srid")
    return parser.parse_args()

if __name__ == "__main__":
    main(**vars(parse_arguments()))
