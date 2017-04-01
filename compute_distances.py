import argparse
import psycopg2
import multiprocessing
from joblib import Parallel, delayed

def main(host, dbname, user, password, lixel_length, srid, search_bandwidth):
    connection_string = "host={0} dbname={1} user={2} password={3}".format(host, dbname, user, password)
    conn = psycopg2.connect(connection_string)
    conn.autocommit = True
    cur = conn.cursor()

    print("Generating midpoints...")
    generate_midpoints(cur, lixel_length, srid)

    print("Computing lixel counts")
    compute_lixel_counts(cur, lixel_length)

    print("Computing lixel distances...")
    compute_lixel_distances(cur, connection_string, lixel_length, search_bandwidth)

def compute_lixel_distances_bucket(connection_string, bucket, lixel_length, search_bandwidth):
    conn = psycopg2.connect(connection_string)
    conn.autocommit = True
    cur = conn.cursor()

    for edge_id in bucket:
        cur.execute("""
            INSERT into lixel_%(lixel_length)s_distances_%(search_bandwidth)s (source_edge, target_edge, distance)
            SELECT LEAST(%(edge_id)s, edge), GREATEST(%(edge_id)s, edge), agg_cost FROM pgr_withPointsDD(
                'SELECT edge_id as id, start_node as source, end_node as target, ST_LENGTH(geom) as cost FROM network_topo_%(lixel_length)s.edge_data
                WHERE ST_DISTANCE((SELECT midpoint from edge_midpoints_%(lixel_length)s as e where e.edge_id = %(edge_id)s), geom) <= %(search_bandwidth)s * 10',
                'SELECT e1.edge_id as pid, e1.edge_id, cast(0.5 as double precision) as fraction from edge_midpoints_%(lixel_length)s as e1
                    WHERE ST_DISTANCE((SELECT e2.midpoint from edge_midpoints_%(lixel_length)s as e2 where e2.edge_id = %(edge_id)s), e1.midpoint) <= %(search_bandwidth)s',
                -%(edge_id)s, %(search_bandwidth)s, directed:=false, details:=true
            ) WHERE node < 0 AND edge != -1
            on conflict (source_edge, target_edge) DO NOTHING
        """, {"edge_id": edge_id, "lixel_length": lixel_length, "search_bandwidth": search_bandwidth})

    cur.close()
    conn.close()

def compute_lixel_distances(cur, connection_string, lixel_length, search_bandwidth):
    cur.execute("""
        CREATE TABLE public.lixel_%(lixel_length)s_distances_%(search_bandwidth)s(
        id serial NOT NULL,
        source_edge integer NOT NULL,
        target_edge integer NOT NULL,
        distance double precision,
        CONSTRAINT lixel_%(lixel_length)s_distances_%(search_bandwidth)s_pkey PRIMARY KEY (id),
        CONSTRAINT lixel_%(lixel_length)s_distances_%(search_bandwidth)s_source_target_unique_constraint UNIQUE (source_edge, target_edge))
    """, {"lixel_length": lixel_length, "search_bandwidth": search_bandwidth})

    cur.execute("SELECT edge_id FROM lixel_%s_count", (lixel_length,))
    rows = cur.fetchall()

    buckets = [[] for i in range(multiprocessing.cpu_count())]
    for row in rows:
        buckets[row[0] % len(buckets)].append(row[0])

    Parallel(n_jobs=-1)(delayed(compute_lixel_distances_bucket)(connection_string, buckets[i], lixel_length, search_bandwidth) for i in range(len(buckets)))

def generate_midpoints(cur, lixel_length, srid):
    cur.execute("""
    CREATE TABLE public.edge_midpoints_%(lixel_length)s(
        edge_id integer NOT NULL,
        midpoint geometry(Point,%(srid)s),
        CONSTRAINT edge_midpoints_%(lixel_length)s_primary_key PRIMARY KEY (edge_id)
    );

    CREATE INDEX edge_midpoints_%(lixel_length)s_spatial_index
        ON public.edge_midpoints_%(lixel_length)s USING gist (midpoint);
    """, {"lixel_length": lixel_length, "srid": srid})

    cur.execute("""INSERT INTO edge_midpoints_%(lixel_length)s SELECT edge_id, ST_LineInterpolatePoint(geom, 0.5) FROM network_topo_%(lixel_length)s.edge_data""", {"lixel_length": lixel_length})

def compute_lixel_counts(cur, lixel_length):
    cur.execute("""
    CREATE TABLE public.lixel_%(lixel_length)s_count(
        edge_id integer NOT NULL,
        count integer NOT NULL,
        CONSTRAINT lixel_%(lixel_length)s_count_primary_key PRIMARY KEY (edge_id)
    );
    """, {"lixel_length": lixel_length})

    cur.execute("""
        INSERT INTO lixel_%(lixel_length)s_count
        SELECT t.edge_id, COUNT(t.gid) FROM
            (SELECT gid,
            (SELECT edge_id
            FROM network_topo_%(lixel_length)s.edge_data
            ORDER BY geom <#> e.wkb_geometry LIMIT 1)
            FROM  events as e) as t
        GROUP BY t.edge_id
    """, {"lixel_length": lixel_length})

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
    parser.add_argument("-sb", type=int, required=True, dest="search_bandwidth",
                        help="search bandwidth")
    return parser.parse_args()

if __name__ == "__main__":
    main(**vars(parse_arguments()))
