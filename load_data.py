import argparse
import subprocess
import psycopg2

def main(host, dbname, user, password, events, network, srid):
    connection_string = "host={0} dbname={1} user={2} password={3}".format(host, dbname, user, password)
    conn = psycopg2.connect(connection_string)
    conn.autocommit = True
    cur = conn.cursor()

    print("Adding necessary extensions to database...")

    cur.execute("""
        CREATE EXTENSION IF NOT EXISTS pgrouting;
        CREATE EXTENSION IF NOT EXISTS postgis; 
        CREATE EXTENSION IF NOT EXISTS postgis_topology; 
        SET search_path = topology,public;
    """)

    print("Loading network shapefile...")
    subprocess.getoutput("ogr2ogr -f PostgreSQL PG:\"{0}\" -a_srs EPSG:{1} -nln public.network -nlt MULTILINESTRING {2}".format(connection_string, srid, network))

    print("Loading events shapefile...")
    subprocess.getoutput("ogr2ogr -f PostgreSQL PG:\"{0}\" -a_srs EPSG:{1} -nln public.events -nlt POINT {2}".format(connection_string, srid, events))

    print("Building network topology...")
    cur.execute("""
        SELECT topology.CreateTopology('network_topo', %s); 
        SELECT topology.AddTopoGeometryColumn('network_topo', 'public', 'network', 'topo_geom', 'LINESTRING');
        UPDATE network SET topo_geom = topology.toTopoGeom(wkb_geometry, 'network_topo', 1, 1.0);
        """, (srid,))

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
    parser.add_argument("-e", required=True, dest="events",
                        help="shapefile for events")
    parser.add_argument("-n", required=True, dest="network",
                        help="shapefile for network")
    parser.add_argument("-s", required=True, dest="srid", help="srid of data")
    return parser.parse_args()

if __name__ == "__main__":
    main(**vars(parse_arguments()))