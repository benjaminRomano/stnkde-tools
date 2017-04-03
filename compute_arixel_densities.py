import argparse
import multiprocessing
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
from joblib import Parallel, delayed

def main(host, dbname, user, password, lixel_length, space_search_bandwidth, time_search_bandwidth, time_type, date_field, srid):
    connection_string = "host={0} dbname={1} user={2} password={3}".format(host, dbname, user, password)
    conn = psycopg2.connect(connection_string)
    conn.autocommit = True

    cur = conn.cursor()
    print("Generating type table...")
    generate_time_type_table(cur, time_type, date_field)

    print("Computing arixel counts...")
    compute_arixel_count(cur, lixel_length, time_type, date_field)

    print("Computing arixel densities...")
    compute_arixel_densities(cur, connection_string, time_type, lixel_length, space_search_bandwidth, time_search_bandwidth)

    print("Creating arixels table...")
    compute_arixels(cur, lixel_length, space_search_bandwidth, time_search_bandwidth, time_type, srid)

def compute_arixels(cur, lixel_length, space_search_bandwidth, time_search_bandwidth, time_type, srid):
    time_type_string = get_time_type_string(time_type)
    table_name = "arixels_{0}_{1}_{2}_{3}".format(lixel_length, time_type_string, space_search_bandwidth, time_search_bandwidth)

    cur.execute(sql.SQL("""
        CREATE TABLE {0} (
            time_id int NOT NULL,
            edge_id int NOT NULL,
            geom geometry(LineString, %(srid)s) NOT NULL,
            count int NOT NULL,
            density double precision NOT NULL,
            height int NOT NULL,
            PRIMARY KEY (time_id, edge_id))
    """).format(sql.Identifier(table_name)), {"srid": srid})

    densities_table_name = "arixel_{0}_{1}_{2}_{3}_densities".format(lixel_length, time_type_string, space_search_bandwidth, time_search_bandwidth)
    network_topo_schema = "network_topo_{0}".format(lixel_length)
    count_table_name = "arixel_{0}_{1}_count".format(lixel_length, time_type_string)

    cur.execute(sql.SQL("""
        INSERT INTO {0}
        (SELECT d.time_id, d.edge_id, ed.geom, 
            c.count,
            CASE 
                WHEN d.density IS NULL THEN 0::double precision 
                ELSE d.density 
            END AS density, 
            d.time_id * 10 
        FROM {1}.edge_data as ed 
            INNER JOIN {2} as d ON d.edge_id = ed.edge_id
            INNER JOIN {3} as c ON c.edge_id = d.edge_id and c.time_id = d.time_id)
    """).format(sql.Identifier(table_name), sql.Identifier(network_topo_schema), sql.Identifier(densities_table_name), sql.Identifier(count_table_name)))

def generate_time_type_table(cur, time_type, date_field):
    time_type_field = get_time_type_field(time_type)
    time_type_table = get_time_type_table(time_type)

    if table_exists(cur, time_type_table):
        return

    cur.execute(sql.SQL("""
        CREATE TABLE {0} (
            id serial not null,
            value int not null,
            PRIMARY KEY (id))
    """).format(sql.Identifier(time_type_table)))

    time_type_data_type = get_time_type_data_type(time_type)

    cur.execute(sql.SQL("""
        INSERT INTO {0} (value)
        SELECT DISTINCT ( EXTRACT({1} FROM {2}::%s))::int as f FROM events ORDER BY f 
    """ % time_type_data_type).format(sql.Identifier(time_type_table), sql.Identifier(time_type_field),
                                      sql.Identifier(date_field)))

def compute_arixel_count(cur, lixel_length, time_type, date_field):
    time_type_field = get_time_type_field(time_type)
    time_type_string = get_time_type_string(time_type)
    time_type_table = get_time_type_table(time_type)
    time_type_data_type = get_time_type_data_type(time_type)

    table_name = "arixel_{0}_{1}_count".format(lixel_length, time_type_string)

    if table_exists(cur, table_name):
        return

    cur.execute(sql.SQL("""
        CREATE TABLE {0}(
            time_id integer NOT NULL,
            edge_id integer NOT NULL,
            count integer NOT NULL,
            PRIMARY KEY (time_id, edge_id));
    """).format(sql.Identifier(table_name)))

    cur.execute(sql.SQL("""
        INSERT INTO {0}
        SELECT time_id, edge_id, COUNT(t.gid) FROM
            (SELECT gid, z.id as time_id,
            (SELECT edge_id
            FROM network_topo_{1}.edge_data
            ORDER BY geom <#> e.wkb_geometry LIMIT 1)
            FROM  events as e
            INNER JOIN {2} as z ON (EXTRACT({3} FROM {4}::%s)) = z.value
            ) as t
        GROUP BY t.edge_id, t.time_id
    """ % time_type_data_type).format(sql.Identifier(table_name), sql.Literal(lixel_length),
                                      sql.Identifier(time_type_table), sql.Identifier(time_type_field),
                                      sql.Identifier(date_field)))

def compute_arixel_densities_bucket(connection_string, bucket, time_type, lixel_length, space_search_bandwidth, time_search_bandwidth):
    conn = psycopg2.connect(connection_string)
    conn.autocommit = True
    cur = conn.cursor()

    lixel_distance_table_name = "lixel_{0}_{1}_distances".format(lixel_length, space_search_bandwidth)
    arixel_densities = {}

    time_type_table = get_time_type_table(time_type)
    cyclic = is_cyclic(time_type)
    cur.execute(sql.SQL("""SELECT id FROM {0}""").format(sql.Identifier(time_type_table)))
    time_ids = [row[0] for row in cur.fetchall()]

    for row in bucket:
        time_id = row[0]
        edge_id = row[1]
        count = row[2]

        cur.execute(sql.SQL("""
            SELECT target_edge, distance FROM {0} WHERE source_edge = %(edge_id)s
            UNION ALL
            SELECT source_edge, distance FROM {0} WHERE target_edge = %(edge_id)s
        """).format(sql.Identifier(lixel_distance_table_name)), {"edge_id": edge_id})

        neighbour_lixels = cur.fetchall()

        add_arixel_density(arixel_densities, time_id, edge_id, 0, 0, count, space_search_bandwidth, time_search_bandwidth)

        for neighbour_lixel in neighbour_lixels:
            for neighbour_time_id in compute_neighbour_time_ids(time_ids, time_id, time_search_bandwidth, cyclic):
                time_distance = compute_time_distance(time_ids, time_id, neighbour_time_id, cyclic)
                add_arixel_density(arixel_densities, neighbour_time_id, neighbour_lixel[0], neighbour_lixel[1], time_distance, count, space_search_bandwidth, time_search_bandwidth)

    return arixel_densities

def compute_arixel_densities(cur, connection_string, time_type, lixel_length, space_search_bandwidth, time_search_bandwidth):
    time_type_string = get_time_type_string(time_type)
    table_name = "arixel_{0}_{1}_{2}_{3}_densities".format(lixel_length, time_type_string, space_search_bandwidth, time_search_bandwidth)
    count_table_name = "arixel_{0}_{1}_count".format(lixel_length, time_type_string)

    cur.execute(sql.SQL("""
        CREATE TABLE {0} (
        time_id integer NOT NULL,
        edge_id integer NOT NULL,
        density double precision,
        PRIMARY KEY (time_id, edge_id))
    """).format(sql.Identifier(table_name)))

    cur.execute(sql.SQL("""
        SELECT time_id, edge_id, count FROM {0} WHERE count > 0
    """).format(sql.Identifier(count_table_name)))

    rows = cur.fetchall()

    buckets = [[] for i in range(multiprocessing.cpu_count())]
    for row in rows:
        buckets[row[0] % len(buckets)].append(row)

    arixel_densities_list = Parallel(n_jobs=-1)(delayed(compute_arixel_densities_bucket)(connection_string, buckets[i], time_type, lixel_length, space_search_bandwidth, time_search_bandwidth) for i in range(len(buckets)))

    values = [(time_id, edge_id, density) for (time_id, edge_id), density in merge_arixel_densities(arixel_densities_list).items()]
    query = "INSERT INTO {0} VALUES %s".format(table_name)
    execute_values(cur, query, values)

def merge_arixel_densities(arixel_densities_list):
    total_arixel_densities = {}
    for arixel_densities in arixel_densities_list:
        for arixel_id, density in arixel_densities.items():
            if arixel_id not in total_arixel_densities:
                total_arixel_densities[arixel_id] = 0
            total_arixel_densities[arixel_id] += density

    return total_arixel_densities

def compute_time_distance(time_ids, time_id1, time_id2, cyclic):
    time_id1_index = time_ids.index(time_id1)
    time_id2_index = time_ids.index(time_id2)

    min_index = min(time_id1_index, time_id2_index)
    max_index = max(time_id1_index, time_id2_index)

    if not cyclic:
        return max_index - min_index

    leftDistance = min_index + len(time_ids) - max_index
    rightDistance = max_index - min_index
    return min(leftDistance, rightDistance)

def compute_neighbour_time_ids(time_ids, time_id, search_bandwidth, cyclic):
    neighbour_time_ids = set()
    time_id_index = time_ids.index(time_id)
    for i in range(search_bandwidth):
        if time_id_index - i < 0 and not cyclic:
            break
        neighbour_time_ids.add(time_ids[(time_id_index - i) % len(time_ids)])

    for i in range(search_bandwidth):
        if time_id_index + i >= len(time_ids) and not cyclic:
            break
        neighbour_time_ids.add(time_ids[(time_id_index + i) % len(time_ids)])

    return neighbour_time_ids

def quartic_curve(distance, search_bandwidth):
    return (3.0/4.0) * (1.0 - ((distance ** 2) / (search_bandwidth ** 2)))

def compute_density(space_distance, time_distance, count, space_search_bandwidth, time_search_bandwidth, kernel):
    return count * (1.0 / (space_search_bandwidth * time_search_bandwidth)) * kernel(time_distance, space_search_bandwidth) * kernel(time_distance, time_search_bandwidth)

def add_arixel_density(arixel_densities, time_id, edge_id, space_distance, time_distance, count, space_search_bandwidth, time_search_bandwidth):
    if (time_id, edge_id) not in arixel_densities:
        arixel_densities[(time_id, edge_id)] = 0

    arixel_densities[(time_id, edge_id)] += compute_density(space_distance, time_distance, count, space_search_bandwidth, time_search_bandwidth, quartic_curve)

def table_exists(cur, table_name):
    cur.execute("SELECT exists(select * from information_schema.tables where table_name=%s)", (table_name,))
    return cur.fetchone()[0]

def validate_time_type(value):
    if value not in ["dw", "h", "w", "m", "s", "y"]:
        raise argparse.ArgumentTypeError("{0} is not a valid time_type value".format(value))

    return value


# TODO: Should probably convert this to class or something...

def get_time_type_data_type(time_type):
    if time_type in ["h"]:
        return "TIME"
    else:
        return "DATE"

def get_time_type_field(time_type):
    if time_type == "dw":
        return "DOW"
    elif time_type == "h":
        return "HOUR"
    elif time_type == "w":
        return "WEEK"
    elif time_type == "m":
        return "MONTH"
    elif time_type == "s":
        return "QUARTER"
    elif time_type == "y":
        return "YEAR"

def get_time_type_table(time_type):
    if time_type == "dw":
        return "days_of_week"
    elif time_type == "h":
        return "hours_of_day"
    elif time_type == "w":
        return "weeks"
    elif time_type == "m":
        return "months"
    elif time_type == "s":
        return "seasons"
    elif time_type == "y":
        return "years"

def get_time_type_string(time_type):
    if time_type == "dw":
        return "by_day_of_week"
    elif time_type == "h":
        return "by_hour_of_day"
    elif time_type == "w":
        return "by_week"
    elif time_type == "m":
        return "by_month"
    elif time_type == "s":
        return "by_season"
    elif time_type == "y":
        return "by_year"

def is_cyclic(time_type):
    return time_type in ["dw", "h", "w", "m", "s"]

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
    parser.add_argument("-ssb", type=int, required=True, dest="space_search_bandwidth",
                        help="space search bandwidth")
    parser.add_argument("-tsb", type=int, required=True, dest="time_search_bandwidth",
                        help="time search bandwidth")
    parser.add_argument("-t", type=validate_time_type, required=True, dest="time_type",
                        help="Time grouping: day of week (dw), hour of day (h), week (w), month (m), season (s), year (y)")
    parser.add_argument("-df", required=True, dest="date_field",
                        help="Date field of events table")
    return parser.parse_args()

if __name__ == "__main__":
    main(**vars(parse_arguments()))
