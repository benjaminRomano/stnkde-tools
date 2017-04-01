import argparse
import multiprocessing
import psycopg2
from psycopg2.extras import execute_values
from joblib import Parallel, delayed

# create dictionary of lixels and their densities
def main(host, dbname, user, password, lixel_length, search_bandwidth, srid):

    connection_string = "host={0} dbname={1} user={2} password={3}".format(host, dbname, user, password)
    conn = psycopg2.connect(connection_string)
    conn.autocommit = True

    cur = conn.cursor()

    print("Computing lixel densities...")
    compute_lixel_densities(cur, connection_string, lixel_length, search_bandwidth)

    print("Creating lixels table...")
    compute_lixels(cur, lixel_length, search_bandwidth, srid)

def compute_lixels(cur, lixel_length, search_bandwidth, srid):
    cur.execute("""
        CREATE TABLE lixels_%(lixel_length)s_%(search_bandwidth)s(
            edge_id int NOT NULL,
            geom geometry(LineString, %(srid)s) NOT NULL,
            count int NOT NULL,
            density double precision NOT NULL)
    """, {"lixel_length": lixel_length, "search_bandwidth": search_bandwidth, "srid": srid})

    cur.execute("""
        INSERT INTO lixels_%(lixel_length)s_%(search_bandwidth)s 
        SELECT ed.edge_id, ed.geom, 
                CASE
                    WHEN lc.count IS NULL THEN 0
                    ELSE lc.count
                END as count,
                CASE
                    WHEN ld.density IS NULL THEN 0::double precision
                    ELSE ld.density
                END AS density
            FROM network_topo_%(lixel_length)s.edge_data ed
                LEFT JOIN lixel_%(lixel_length)s_count lc ON lc.edge_id = ed.edge_id
                LEFT JOIN lixel_%(lixel_length)s_%(search_bandwidth)s_densities ld ON ld.id = ed.edge_id;
    """, {"lixel_length": lixel_length, "search_bandwidth": search_bandwidth})

def compute_lixel_densities_bucket(connection_string, bucket, lixel_length, search_bandwidth):
    conn = psycopg2.connect(connection_string)
    conn.autocommit = True

    cur = conn.cursor()
    lixel_densities = {}

    for row in bucket:
        edge_id = row[0]
        count = row[1]

        cur.execute("""
            SELECT target_edge, distance FROM lixel_%(lixel_length)s_%(search_bandwidth)s_distances WHERE source_edge = %(edge_id)s
            UNION ALL
            SELECT source_edge, distance FROM lixel_%(lixel_length)s_%(search_bandwidth)s_distances WHERE target_edge = %(edge_id)s
        """, {"lixel_length": lixel_length, "search_bandwidth": search_bandwidth, "edge_id": edge_id})

        neighbour_lixels = cur.fetchall()

        add_lixel_density(lixel_densities, edge_id, 0, count, search_bandwidth)

        for neighbour_lixel in neighbour_lixels:
            add_lixel_density(lixel_densities, neighbour_lixel[0], neighbour_lixel[1], count, search_bandwidth)

    return lixel_densities

def compute_lixel_densities(cur, connection_string, lixel_length, search_bandwidth):
    lixel_densities = {}
    cur.execute("""
        CREATE TABLE public.lixel_%(lixel_length)s_%(search_bandwidth)s_densities (
        id integer NOT NULL,
        density double precision,
        CONSTRAINT lixel_%(lixel_length)s_%(search_bandwidth)s_densities_pkey PRIMARY KEY (id))
    """, {"lixel_length": lixel_length, "search_bandwidth": search_bandwidth})

    cur.execute("""SELECT edge_id, count FROM lixel_%s_count WHERE count > 0""", (lixel_length,))
    rows = cur.fetchall()

    buckets = [[] for i in range(multiprocessing.cpu_count())]
    for row in rows:
        buckets[row[0] % len(buckets)].append(row)

    lixel_densities_list = Parallel(n_jobs=-1)(delayed(compute_lixel_densities_bucket)(connection_string, buckets[i], lixel_length, search_bandwidth) for i in range(len(buckets)))

    values = [(edge_id, density) for edge_id, density in merge_lixel_densities(lixel_densities_list).items()]
    query = "INSERT INTO lixel_{0}_{1}_densities VALUES %s".format(lixel_length, search_bandwidth)
    execute_values(cur, query, values)

def merge_lixel_densities(lixel_densities_list):
    total_lixel_densities = {}
    for lixel_densities in lixel_densities_list:
        for edge_id, density in lixel_densities.items():
            if edge_id not in total_lixel_densities:
                total_lixel_densities[edge_id] = 0
            total_lixel_densities[edge_id] += density

    return total_lixel_densities

def quartic_curve(distance, search_bandwidth):
    return (3.0/4.0) * (1.0 - ((distance ** 2) / (search_bandwidth ** 2)))

def compute_density(distance, num_events, search_radius, kernel):
    return num_events * (1.0 / search_radius) * kernel(distance, search_radius)

def add_lixel_density(lixel_densities, edge_id, distance, num_events, search_bandwidth):
    if edge_id not in lixel_densities:
        lixel_densities[edge_id] = 0

    lixel_densities[edge_id] += compute_density(distance, num_events, search_bandwidth, quartic_curve)

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
