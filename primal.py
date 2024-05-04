from config import *
from utils import *
# from scripts import *
import argparse
import os
import subprocess
import json

class GetPrimal:
    def __init__(self):
        self.topology_villages = []
        self.topology_villages_single = []
        pass
    def run(self):

        print("\n------------creating topology-------------\n")
        create_topo(conn, "adjacent_villages","algo_village_midlines_topo","new_village_boundaries")
        #---------------------------
        print("\n------------adding void polygon-------------\n")
        self.add_void_polygon(conn)
        print("\n------------finding centroid-------------\n")
        self.find_centroid(conn)
        # ---------------
        print("\n------------finding nodes degree-------------\n")
        self.find_nodes_degree(conn)
        print("\n------------finding triple points-------------\n")
        self.find_triple_points(conn)
        print("\n------------dropping perpendiculars-------------\n")
        self.drop_perpendiculars(conn)
        print("\n------------splitting boundary-------------\n")
        self.split_boundary(conn) 
        # fine till here
        # the following functions need inspection
        print("\n------------splitting midlines-------------\n")
        self.split_midlines(conn)
        print("\n------------deleting duplicate entries-------------\n")
        self.delete_duplicate_rows(conn)
        print("\n------------finding boundary midlines-------------\n")
        self.find_boundary_midlines(conn)
        print("\n------------combining splitted boundaries-------------\n")
        self.combine_splitted_boundaries(conn)
        print("\n------------END-------------\n")

    def combine_splitted_boundaries(self,conn):
        sql_query = f"""
            drop table if exists algo_test.combined_splitted_boundaries;
            create table algo_test.combined_splitted_boundaries(
                village varchar(200),
                geom Geometry(LineString, 32643)
            );
        """
        with conn.connection().cursor() as curs:
            curs.execute(sql_query)
        conn.connection().commit()

        for vil in village_list:
            sql_query = f"""
                insert into algo_test.combined_splitted_boundaries(village, geom)
                select * from algo_test.{vil}_split;
            """   
            with conn.connection().cursor() as curs:
                curs.execute(sql_query)
            conn.connection().commit()

    def add_void_polygon(self,conn):
        sql_query = f"""
            create schema if not exists algo_test;
            
            drop table if exists algo_test.aaa;
            create table algo_test.aaa as
            select
                st_exteriorring((st_dump(geom)).geom) as geom
            from
                adjacent_villages.new_village_boundaries;

            -- I was confused hahaha

            insert into adjacent_villages.new_village_boundaries(village, geom)
            select
                'void' as village,
                st_difference(
                    st_buffer(st_union(a.geom),100),
                    st_union(a.geom)
                ) as geom
            from
                adjacent_villages.new_village_boundaries as a
            ;
            
            drop table if exists adjacent_villages.void_boundary;
            create table adjacent_villages.void_boundary as
            select
                st_union(geom) as geom
            from
                adjacent_villages.new_village_boundaries
            ;
        """
        with conn.connection().cursor() as curs:
            curs.execute(sql_query)
        conn.connection().commit()

    def find_centroid(self,conn):
        sql_query = """
            create schema if not exists algo_test;
            drop table if exists algo_test.village_centroids;
            create table algo_test.village_centroids (
                village varchar(200),
                geom Geometry(Point, 32643)
            );
        """
        with conn.connection().cursor() as curs:
            curs.execute(sql_query)
        conn.connection().commit()
        for village in village_list:
            sql_query = f"""
                insert into algo_test.village_centroids (village, geom)
                select
                    '{village}' as village,
                    st_centroid(vil.geom) as geom
                    
                from
                    adjacent_villages.{village}_boundary as vil;
            """
            with conn.connection().cursor() as curs:
                curs.execute(sql_query)
            conn.connection().commit()

    def find_nodes_degree(self,conn):
        sql_query = f"""
            drop table if exists algo_test.nodes_with_degree;
            create table algo_test.nodes_with_degree as

            select
                count(edge.edge_id) as degree,
                node.node_id as node_id,
                node.geom as geom
            from
                algo_village_midlines_topo.edge as edge,
                algo_village_midlines_topo.node as node
            where
                (edge.start_node = node.node_id
                or
                edge.end_node = node.node_id)

            group by
                node.node_id,
                node.geom
            ;

        """

        with conn.connection().cursor() as curs:
            curs.execute(sql_query)
        conn.connection().commit()

    def find_triple_points(self,conn):
        sql_query = """
            drop table if exists algo_test.triple_points;
            create table algo_test.triple_points as
            select
                node_id,
                degree,
                geom
            from
                algo_test.nodes_with_degree
            where
                degree = 3
            ;
        """
        with conn.connection().cursor() as curs:
            curs.execute(sql_query)
        conn.connection().commit()

    def drop_perpendiculars(self,conn):
        # sql_query = """
        #     alter table algo
        # """
        sql_query = """
            select * from algo_test.triple_points;
        """
        with conn.connection().cursor() as curs:
            curs.execute(sql_query)
            trip_pts = curs.fetchall()
        conn.connection().commit()

        sql_query = """
            drop table if exists algo_test.village_topology_graph;
            create table algo_test.village_topology_graph (
                village_1 varchar(200),
                village_2 varchar(200),
                geom Geometry(LineString, 32643)
            );
        """
        with conn.connection().cursor() as curs:
            curs.execute(sql_query)
        conn.connection().commit()

        for vil in village_list:
            sql_query = f"""
                drop table if exists algo_test.{vil}_perp_base_points;
                create table algo_test.{vil}_perp_base_points (
                    geom Geometry(Point, 32643)
                )
            """
            with conn.connection().cursor() as curs:
                curs.execute(sql_query)
            conn.connection().commit()

        # adding the columns for adjacent villages for triple points
        sql_query = f"""
            alter table algo_test.triple_points add village1 varchar(200);
            alter table algo_test.triple_points add village2 varchar(200);
            alter table algo_test.triple_points add village3 varchar(200);
        """
        with conn.connection().cursor() as curs:
            curs.execute(sql_query)
        conn.connection().commit()

        for pt in trip_pts:
            # print(f"pt[0]: {pt[0]}") # this is node_id
            # print(f"pt[1]: {pt[1]}") # this is degree
            # print(f"pt[2]: {pt[2]}") # this is geom
            sql_query = f"""
                select
                    a.village
                from
                    adjacent_villages.new_village_boundaries as a,
                    algo_test.nodes_with_degree as b
                where
                    st_intersects(b.geom,a.geom)
                    and
                    b.node_id = {pt[0]}
                ;
            """
            with conn.connection().cursor() as curs:
                curs.execute(sql_query)
                adj_villages = curs.fetchall()
            conn.connection().commit()
            # print(f"--------------villages------------\n{adj_villages}")
            # print(f"number of villages = {len(adj_villages)}")

            # dropping perpendiculars
            for vil in adj_villages:
                # sql_query = f"""
                #     insert into algo_test.triple_points
                #     select
                #         vil[0]
                # """

                sql_query = f"""
                    insert into algo_test.{vil[0]}_perp_base_points
                    select
                        st_closestpoint(st_exteriorring(a.geom), b.geom) as geom
                    from
                        adjacent_villages.{vil[0]}_boundary as a,
                        algo_test.nodes_with_degree as b
                    where
                        b.node_id = {pt[0]}
                    ;
                """
                with conn.connection().cursor() as curs:
                    curs.execute(sql_query)
                conn.connection().commit()

            # creating villages topology
            if len(adj_villages) == 3:
                for i in range(3):
                    my_list = [f'{adj_villages[i%3][0]}',f'{adj_villages[(i+1)%3][0]}']
                    my_list.sort()
                    self.topology_villages.append(my_list)
                    # print(f"sorted list :{my_list}")
                    sql_query = f"""
                        update algo_test.triple_points as a
                        set village{i+1} = '{adj_villages[i][0]}'
                        where 
                            a.node_id = {pt[0]};
                        --from
                        --    algo_test.triple_points as a
                        --where
                        --    a.node_id = {pt[0]}
                        
                    """
                    with conn.connection().cursor() as curs:
                        curs.execute(sql_query)
                    conn.connection().commit()

                    # print("hi")
                    sql_query = f"""
                        insert into algo_test.village_topology_graph (village_1, village_2, geom)
                        with start_end_points as (
                            select
                                (select
                                    geom as start_pt
                                from
                                    algo_test.village_centroids
                                where
                                    village = '{my_list[0]}') as start_pt,
                                
                                (select
                                    geom as end_pt
                                from
                                    algo_test.village_centroids
                                where
                                    village = '{my_list[1]}') as end_pt          
                        )

                        select
                            '{my_list[0]}' as village_1,
                            '{my_list[1]}' as village_2,
                            st_makeline((select start_pt from start_end_points),(select end_pt from start_end_points)) as geom
                        from 
                            start_end_points
                        ;
                    """
                    with conn.connection().cursor() as curs:
                        curs.execute(sql_query)
                    conn.connection().commit()

                
                    
            if len(adj_villages)==2:
                for i in range(1):
                    my_list = [f'{adj_villages[i%3][0]}',f'{adj_villages[(i+1)%3][0]}']
                    my_list.sort()
                    # print(my_list)
                    self.topology_villages.append(my_list)
                    sql_query = f"""
                        update algo_test.triple_points as a
                        set village{i+1} = '{adj_villages[i][0]}'
                        where 
                            a.node_id = {pt[0]};

                        update algo_test.triple_points as a
                        set village{i+2} = '{adj_villages[i+1][0]}'
                        where
                            a.node_id = {pt[0]};
                    """
                    with conn.connection().cursor() as curs:
                        curs.execute(sql_query)
                    conn.connection().commit()
                    # print("hi2")
                    sql_query = f"""
                        insert into algo_test.village_topology_graph (village_1, village_2, geom)
                        with start_end_points as (
                            select
                                (select
                                    geom as start_pt
                                from
                                    algo_test.village_centroids
                                where
                                    village = '{my_list[0]}') as start_pt,
                                
                                (select
                                    geom as end_pt
                                from
                                    algo_test.village_centroids
                                where
                                    village = '{my_list[1]}') as end_pt          
                        )

                        select
                            '{my_list[0]}' as village_1,
                            '{my_list[1]}' as village_2,
                            st_makeline((select start_pt from start_end_points),(select end_pt from start_end_points)) as geom
                        from 
                            start_end_points
                        ;
                    """
                    with conn.connection().cursor() as curs:
                        curs.execute(sql_query)
                    conn.connection().commit()

    def split_boundary(self,conn):
        for village in village_list:
            # print("debug1------------------")
            # print(village)
            sql_query = f"""
                drop table if exists algo_test.{village}_ring;
                create table algo_test.{village}_ring as
                select 
                    st_exteriorring(geom) as geom
                from
                    adjacent_villages.{village}_boundary
                ;

                drop table if exists algo_test.{village}_splitted_boundary;
                create table algo_test.{village}_splitted_boundary as
                with blade as(
                    select st_buffer(geom, 0.01) as geom
                    from algo_test.{village}_perp_base_points
                )
                select
                    (st_dump(st_difference(line.geom,st_union(blade.geom)))).geom as geom
                from
                    algo_test.{village}_ring as line,
                    blade
                group by line.geom
                ;

                drop table if exists algo_test.{village}_split;
                create table algo_test.{village}_split (
                    village varchar(200),
                    geom Geometry(LineString, 32643)
                );

                insert into algo_test.{village}_split
                select
                    '{village}' as village,
                    (st_dump(st_linemerge(st_collect(line.geom)))).geom as geom
                from
                    algo_test.{village}_splitted_boundary as line
                ;

                drop table if exists {village}_splitted_boundary;
            """
            with conn.connection().cursor() as curs:
                curs.execute(sql_query)
            conn.connection().commit()

    def split_midlines(self,conn):
        for village in village_list:
            sql_query = f"""
                drop table if exists algo_test.village_midlines;
                create table algo_test.village_midlines as
                select
                    st_exteriorring(geom) as geom
                from
                    adjacent_villages.new_village_boundaries
                where
                    village = '{village}'
                ;

                drop table if exists algo_test.{village}_splitted_midlines;
                create table algo_test.{village}_splitted_midlines as
                with blade as(
                    select st_buffer(geom,0.01) as geom
                    from algo_test.triple_points
                )
                select
                    '{village}' as village,
                    (st_dump(st_difference(line.geom,st_union(blade.geom)))).geom as geom
                from
                    algo_test.village_midlines as line,
                    blade
                group by
                    line.geom
                ;
            """
            with conn.connection().cursor() as curs:
                curs.execute(sql_query)
            conn.connection().commit()
        
        sql_query = f"""

            drop table if exists algo_test.splitted_midlines;
            create table algo_test.splitted_midlines as
            with blade as(
                select st_buffer(geom,0.01) as geom
                from algo_test.triple_points
            )
            select
                (st_dump(st_linemerge(st_difference(st_union(line.geom),st_collect(blade.geom))))).geom as geom
            from
                algo_test.aaa as line,
                blade
            ;
        """
        with conn.connection().cursor() as curs:
            curs.execute(sql_query)
        conn.connection().commit()

    def delete_duplicate_rows(self,conn):
        print("topology villages are")
        print(self.topology_villages)
        [self.topology_villages_single.append(x) for x in self.topology_villages if x not in self.topology_villages_single]
        # sql_query = f"""
        #     delete from algo_test.village_topology_graph as top
        #     where(village_1, village_2) in (
        #         select top.village_1
        #     )
        # """
        # with conn.connection().cursor() as curs:
        #     curs.execute(sql_query)
        # conn.connection().commit()
        sql_query = """
            drop table if exists algo_test.village_topology_nice;
            create table algo_test.village_topology_nice (
                village_1 varchar(200),
                village_2 varchar(200),
                geom Geometry(LineString, 32643)
            );
        """
        with conn.connection().cursor() as curs:
            curs.execute(sql_query)
        conn.connection().commit()

        for twovills in self.topology_villages_single:
            sql_query = f"""
                insert into algo_test.village_topology_nice (village_1, village_2, geom)
                with start_end_points as (
                    select
                        (select
                            geom as start_pt
                        from
                            algo_test.village_centroids
                        where
                            village = '{twovills[0]}') as start_pt,
                        
                        (select
                            geom as end_pt
                        from
                            algo_test.village_centroids
                        where
                            village = '{twovills[1]}') as end_pt          
                )

                select
                    '{twovills[0]}' as village_1,
                    '{twovills[1]}' as village_2,
                    st_makeline((select start_pt from start_end_points),(select end_pt from start_end_points)) as geom
                from 
                    start_end_points
                ;
            """
            with conn.connection().cursor() as curs:
                curs.execute(sql_query)
            conn.connection().commit()

        # print(f"original list: {self.topology_villages}\n")
        # print(f"list after deleting: {self.topology_villages_single}\n")

    def find_boundary_midlines(self,conn):
        sql_query = """
            drop table if exists algo_test.village_adj_midlines;
            create table algo_test.village_adj_midlines(
                village1 varchar(200),
                village2 varchar(200),
                geom Geometry(LineString, 32643)
            )
        """
        with conn.connection().cursor() as curs:
            curs.execute(sql_query)
        conn.connection().commit()

        sql_query = f"""
            drop table if exists algo_test.conflict_vbs;
            create table algo_test.conflict_vbs(
                geom Geometry(LineString, 32643),
                village1 varchar(200),
                village2 varchar(200)
            )
        """
        with conn.connection().cursor() as curs:
            curs.execute(sql_query)
        conn.connection().commit()

        for twovills in self.topology_villages_single:
            print(f"village1 : {twovills[0]}")
            print(f"village2 : {twovills[1]}")
            print("-----------------------------")
            sql_query = f"""
                insert into algo_test.conflict_vbs(geom, village1, village2)
                with v1geom as(
                    select
                        village,
                        geom
                    from
                        adjacent_villages.new_village_boundaries
                    where village = '{twovills[0]}'
                ),

                v2geom as(
                    select
                        village,
                        geom
                    from
                        adjacent_villages.new_village_boundaries
                    where village = '{twovills[1]}'
                )

                select
                    lines.geom as geom,
                    '{twovills[0]}' as village1,
                    '{twovills[1]}' as village2
                from
                    algo_test.splitted_midlines as lines,
                    v1geom,
                    v2geom
                where
                    st_containsproperly(st_union(v1geom.geom,v2geom.geom), lines.geom)
                ;
            """
            with conn.connection().cursor() as curs:
                curs.execute(sql_query)
            conn.connection().commit()

        print("Topology villages single: ")
        print(self.topology_villages_single)
        with open("file.json", 'w') as f:
            # indent=2 is not needed but makes the file human-readable 
            # if the data is nested
            json.dump(self.topology_villages_single, f, indent=1 ) 



# ('deolanakh',)

if __name__=="__main__":

    config = Config()
    pgconn_obj = PGConn(config)
    conn = pgconn_obj

    village_list = []

    with open("./config/villages1.txt", 'r') as file:
        for line in file:
            village_list.append(line.strip())

    region = 'algo_test'

    print(village_list)

    create_primal = GetPrimal()
    create_primal.run()