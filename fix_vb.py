from config import *
from utils import *
# from scripts import *
import argparse
import os
import subprocess
import json

class fixVB:
    def __init__(self):
        pass
    
    def run(self):
        sql_query = f"""
            create schema if not exists fixed_vb;
        """
        with conn.connection().cursor() as curs:
            curs.execute(sql_query)
        conn.connection().commit()
        self.setUp()

        pass

    def setUp(self):
        
        sql_query = f"""
            --select DropTopology('labelled_midline_topo');

            drop table if exists boundary_surveys.deolanabk_deolanakh_midline;
            drop table if exists boundary_surveys.deolanabk_deolanakh;
            create table boundary_surveys.deolanabk_deolanakh_midline as
            select * from boundary_surveys.all_labelled_midlines
            where
                village1 = 'deolanabk'
                and
                village2 = 'deolanakh'
            ;
        """
        with conn.connection().cursor() as curs:
            curs.execute(sql_query)
        conn.connection().commit()
        
        # create topology for the midline between two villages
        create_topo(conn, "boundary_surveys", "dbdk_midline_topo","deolanabk_deolanakh_midline", 0.1)

        # we will add extra nodes in this topology
        create_topo(conn, "adjacent_villages", "deolanabk_fix_topo", "deolanabk_faces", 0.01)

        # extract the deolanabk-deolanakh original boundary from deolanabk
        sql_query = f"""
            -- find the midline between two villages
            drop table if exists boundary_surveys.trial_boundary;
            create table boundary_surveys.trial_boundary as
            select
                geom,
                village1,
                village2
            from
                algo_test.conflict_vbs
            where
                village1 = 'deolanabk'
                and
                village2 = 'deolanakh'
            ;

            -- extract the relevant boundary line of village
            drop table if exists boundary_surveys.original_boundary_line;
            create table boundary_surveys.original_boundary_line as
            with distances as(
                select
                    line.geom,
                    (
                        st_distance(st_startpoint(midline.geom),line.geom) + 
                        st_distance(st_lineinterpolatepoint(midline.geom,0.25),line.geom)+
                        st_distance(st_lineinterpolatepoint(midline.geom,0.50),line.geom)+
                        st_distance(st_lineinterpolatepoint(midline.geom,0.75),line.geom)+
                        st_distance(st_endpoint(midline.geom),line.geom)
                    ) as dist
                from
                    boundary_surveys.trial_boundary as midline,
                    algo_test.deolanabk_split as line
            ),
            min_dist as (
                select
                    min(distances.dist) as mindist
                from
                    distances
            )
            select
                distances.geom,
                distances.dist
            from
                distances,
                min_dist
            where
                distances.dist = min_dist.mindist
            ;
        """
        with conn.connection().cursor() as curs:
            curs.execute(sql_query)
        conn.connection().commit()

        # topologise the deolanabk original boundary corresponding to the midline
        create_topo(conn, "boundary_surveys", "dbdk_original_topo","original_boundary_line", 0.1)

        
        # sql_query = f"""
        #     drop table if exists fixed_vb.projection_points;
        #     create table fixed_vb.projection_points as
        #     select
        #         st_closestpoint(line.geom, pts.geom) as geom
        #     from
        #         boundary_surveys.original_boundary_line as line,
        #         .
        # """


        # first we will project the midline points on the original village topology
        sql_query = f"""
            drop table if exists fixed_vb.projection_points;
            create table fixed_vb.projection_points (
                geom Geometry(Point, 32643),
                village1 varchar(200),
                village2 varchar(200)
            );
        """
        with conn.connection().cursor() as curs:
            curs.execute(sql_query)
        conn.connection().commit()

        sql_query = f"""
            ALTER TABLE boundary_surveys.all_labelled_midlines
            ADD COLUMN if not exists sr_no SERIAL PRIMARY KEY
            ;

            select * from boundary_surveys.all_labelled_midlines
            where
                village1 = 'deolanabk'
                and
                village2 = 'deolanakh'
            ;
        """
        with conn.connection().cursor() as curs:
            curs.execute(sql_query)
            linestrings = curs.fetchall()
        conn.connection().commit()

        for line in linestrings:
            print(f"line4 :{line[4]}") # this is serial number
            # break
            sql_query = f"""
                --insert into fixed_vb.projection_points(
                --    geom, village1, village2
                --)
                --select
                --    st_closestpoint(line.geom,st_startpoint(source.geom)),
                --    'deolanabk' as village1,
                --    'deolanakh' as village2
                --from
                --    boundary_surveys.all_labelled_midlines as source,
                --    algo_test.deolanabk_ring as line
                --where
                --    sr_no = {line[4]}
                --;

                --insert into fixed_vb.projection_points(
                --    geom, village1, village2
                --)
                --select
                --    st_closestpoint(line.geom, st_endpoint(source.geom)),
                --    'deolanabk' as village1,
                --    'deolanakh' as village2
                --from
                --    boundary_surveys.all_labelled_midlines as source,
                --    algo_test.deolanabk_ring as line
                --where
                --    sr_no = {line[4]}
                --;

                drop table if exists fixed_vb.projection_points;
                create table fixed_vb.projection_points as
                select
                    st_closestpoint(line.geom, points.geom) as geom
                from
                    boundary_surveys.original_boundary_line as line,
                    dbdk_midline_topo.node as points
            """
            with conn.connection().cursor() as curs:
                curs.execute(sql_query)
            conn.connection().commit()
            pass

        sql_query = f"""
            select topogeo_addpoint('deolanabk_fix_topo', points.geom, 0.05)
            from fixed_vb.projection_points as points;
        """
        with conn.connection().cursor() as curs:
            curs.execute(sql_query)
        conn.connection().commit()
        
        # now we will project the original boundary on the midlines topology
        sql_query = f"""
            drop table if exists fixed_vb.projection_points;
            create table fixed_vb.projection_points as
            select
                st_closestpoint(line.geom, pts.geom) as geom
            from
                boundary_surveys.trial_boundary as line,
                dbdk_original_topo.node as pts
            ;
        """
        with conn.connection().cursor() as curs:
            curs.execute(sql_query)
        conn.connection().commit()
        # return
        sql_query = f"""
            select topogeo_addpoint('dbdk_midline_topo', points.geom, 0.1)
            from fixed_vb.projection_points as points;
        """
        with conn.connection().cursor() as curs:
            curs.execute(sql_query)
        conn.connection().commit()



        pass

if __name__=="__main__":

    config = Config()
    pgconn_obj = PGConn(config)
    conn = pgconn_obj

    village_list = []

    with open("./config/villages1.txt", 'r') as file:
        for line in file:
            village_list.append(line.strip())

    region = 'boundary_surveys'
    print(village_list)

    create_primal = fixVB()
    create_primal.run()