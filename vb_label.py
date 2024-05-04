from config import *
from utils import *
# from scripts import *
import argparse
import os
import subprocess
import json

class labelVB:
    def __init__(self):
        self.topology_villages = []
        self.topology_villages_single = []
    
    def run(self):

        with open("file.json", 'r') as f:
            adj = json.load(f)
        
        # sql_query = f"""
        #     drop table if exists boundary_surveys.combined_labels;
        #     create table boundary_surveys.combined_labels (
        #         geom Geometry(LineString, 32643),
        #         village varchar(200),
        #         survey_no varchar(20)
        #     );
        # """
        # with conn.connection().cursor() as curs:
        #     curs.execute(sql_query)
        # conn.connection().commit()

        # for village in village_list:
        #     self.label_midline_boundaries(village, adj)
        #     self.label_original_boundaries(village)
        #--------------------------------------------------------------------for testing comment till here
            # for labelling original boundaries
            #   1. first extract the narrow surveys on
            #      boundaries for the village
            #   2. then take intersection of village_split
            #      and narrows surveys buffered by 0.01 meter
            #   3. label these by the survey_no's (narrows)
            #   4. then insert these into temporary table
            #   5. then take the difference of village_split
            #      union and labelled narrows buffered
            #      and then insert into the combined
            #      village boundary table with plot label
            #   6. now insert the temporary table into
            #      the combined village boundary table
            # break
        self.label_midlines(adj)

    def label_midlines(self,adj):

        sql_query = f"""
            drop table if exists boundary_surveys.tempo;
            create table boundary_surveys.tempo (
                geom Geometry(Linestring, 32643),
                village1 varchar(200),
                village2 varchar(200),
                label varchar(20)
            )
        """
        with conn.connection().cursor() as curs:
            curs.execute(sql_query)
        conn.connection().commit()

        print("---------LABEL MIDLINES STARTED-----------")
        for village in village_list:
            sql_query = f"""
                ALTER TABLE boundary_surveys.{village}_labelled_boundary
                ADD COLUMN if not exists sr_no SERIAL PRIMARY KEY;
            """
            with conn.connection().cursor() as curs:
                curs.execute(sql_query)
            conn.connection().commit()

        adj_list = [inner_list for inner_list in adj if 'void' not in inner_list]
        print(adj_list)
        for item in adj_list:
            sql_query = f"""
                select
                    *
                from
                    boundary_surveys.{item[0]}_labelled_boundary
                where
                    village1 = '{item[0]}'
                    and
                    village2 = '{item[1]}'
                ;
            """
            with conn.connection().cursor() as curs:
                curs.execute(sql_query)
                boundary_strings = curs.fetchall()
            conn.connection().commit()
            for boundary_string in boundary_strings:
                # print(boundary_string[0]) # this is geom 
                # print(boundary_string[1])
                # print(boundary_string[2])
                # print(boundary_string[3])
                # print(boundary_string[4]) # this is sr_no
                sql_query = f"""
                    insert into boundary_surveys.tempo (
                        geom, village1, village2, label
                    )
                    select
                        st_intersection(v1.geom, st_buffer(v2.geom, 0.01, 'endcap=flat join=round')) as geom,
                        '{item[0]}' as village1,
                        '{item[1]}' as village2,
                        CASE
                            WHEN v1.survey_no = 'plot' and v2.survey_no = 'plot'
                                THEN 's-s'
                            WHEN v1.survey_no = 'plot' and v2.survey_no != 'plot'
                                THEN 'r-s'
                            WHEN v1.survey_no != 'plot' and v2.survey_no != 'plot'
                                THEN 'r-r'
                            ELSE
                                'r-s'
                        END as label
                    from
                        boundary_surveys.{item[1]}_labelled_boundary as v2,
                        boundary_surveys.{item[0]}_labelled_boundary as v1
                    where
                        v2.village1 = '{item[0]}'
                        and
                        v2.village2 = '{item[1]}'
                        and
                        v1.sr_no = {boundary_string[4]}
                    ;
                """
                with conn.connection().cursor() as curs:
                    curs.execute(sql_query)
                conn.connection().commit()

        sql_query = f"""
            drop table if exists boundary_surveys.all_labelled_midlines;
            create table boundary_surveys.all_labelled_midlines as
            select
                *
            from
                boundary_surveys.tempo as tempo
            where
                st_length(tempo.geom)>0
            ;
            drop table if exists boundary_surveys.tempo;
        """
        with conn.connection().cursor() as curs:
            curs.execute(sql_query)
        conn.connection().commit()

        void_list = [inner_list for inner_list in adj if 'void' in inner_list]
        print(void_list)
        for vil in void_list:
            vil.remove('void')
            sql_query = f"""
                insert into boundary_surveys.all_labelled_midlines(
                    geom, village1, village2, label
                )
                select
                    geom,
                    village1,
                    village2,
                    'void' as label
                from
                    boundary_surveys.{vil[0]}_labelled_boundary
                where
                    village1 = 'void'
                    or
                    village2 = 'void'
                ;
            """
            with conn.connection().cursor() as curs:
                curs.execute(sql_query)
            conn.connection().commit()


    def label_original_boundaries(self, village):
        # create a table to store the labelled boundaries
        # for that particular village
        sql_query = f"""
            -- now we will form a table for each village
            -- which will contain the labelling for that
            -- particular village
            drop table if exists boundary_surveys.{village}_original_labelled;
            create table boundary_surveys.{village}_original_labelled (
                geom Geometry(LineString, 32643),
                village varchar(200),
                survey_no varchar(20)
            );
        """
        with conn.connection().cursor() as curs:
            curs.execute(sql_query)
        conn.connection().commit()

        sql_query = f"""
            -- create a temporary table
            drop table if exists boundary_surveys.temp;
            create table boundary_surveys.temp (
                geom Geometry(LineString, 32643),
                village varchar(200),
                survey_no varchar(20)
            );

            -- extract the boundary narrow surveys
            -- get all the boundary surveys
            drop table if exists boundary_surveys.boundary_surveys;
            create table boundary_surveys.boundary_surveys as
            select
                faces.*
            from
                adjacent_villages.{village}_faces as faces,
                adjacent_villages.{village}_boundary as vb
            where
                st_intersects(faces.geom, st_exteriorring(vb.geom))
            ;

            -- get all the narrow faces at boundary
            drop table if exists boundary_surveys.boundary_narrows;
            create table boundary_surveys.boundary_narrows as
            select
                row_number() over(order by (select NULL)) as sr_no,
                boundary_surveys.geom,
                boundary_surveys.survey_no
            from
                boundary_surveys.{village}_boundary_surveys as boundary_surveys
            where
                survey_no = 'रस्ता'
                or
                left(survey_no,1) = 'S'
        """
        with conn.connection().cursor() as curs:
            curs.execute(sql_query)
        conn.connection().commit()
        
        sql_query = f"""
            ALTER TABLE algo_test.{village}_split
            ADD COLUMN if not exists sr_no SERIAL PRIMARY KEY;
        """
        with conn.connection().cursor() as curs:
            curs.execute(sql_query)
        conn.connection().commit()

        sql_query = f"""
            select * from algo_test.{village}_split;
        """
        with conn.connection().cursor() as curs:
            curs.execute(sql_query)
            borders = curs.fetchall()
        conn.connection().commit()

        for border in borders:
            # print(f"{border[0]}") ---> this is village
            # print(f"{border[1]}") ---> this is geom
            # print(f"{border[2]}") ---> this is the sr_no
            # for each border
            #   for each narrow face
            #       find the intersection of
            #       border and narrow face
            sql_query = f"""
                select
                    *
                from
                    boundary_surveys.boundary_narrows
                ;
            """
            with conn.connection().cursor() as curs:
                curs.execute(sql_query)
                narrow_faces = curs.fetchall()
            conn.connection().commit()

            for narrow_face in narrow_faces:

                # narrow_face[0] is the serial number

                sql_query = f"""
                    insert into boundary_surveys.temp (
                        geom, village, survey_no
                    )
                    select
                        (st_dump(st_intersection(
                            st_union(border.geom),
                            st_buffer(st_union(narrow_b.geom),0.01)
                        ))).geom as geom,
                        '{village}' as village,
                        '{narrow_face[2]}' as survey_no
                    from
                        boundary_surveys.boundary_narrows as narrow_b,
                        algo_test.{village}_split as border
                    where
                        border.sr_no = {border[2]}
                        and
                        narrow_b.sr_no = {narrow_face[0]}
                    ;
                """
                with conn.connection().cursor() as curs:
                    curs.execute(sql_query)
                conn.connection().commit()

            sql_query = f"""
                insert into boundary_surveys.{village}_original_labelled (
                    geom, survey_no, village
                )
                select
                    b_surveys.geom as geom,
                    b_surveys.survey_no as survey_no,
                    '{village}' as village
                from
                    boundary_surveys.temp as b_surveys
                ;

                insert into boundary_surveys.{village}_original_labelled(
                    geom, survey_no, village
                )
                with blade as(
                    select st_buffer(geom,0.01) as geom
                    from boundary_surveys.temp as narrows
                )
                select
                    (st_dump(st_linemerge(st_difference(st_union(line.geom),st_collect(blade.geom))))).geom as geom,
                    'plot' as survey_no,
                    '{village}' as village
                from
                    algo_test.{village}_split as line,
                    blade
                where
                    line.sr_no = {border[2]}
                ;
            """
            with conn.connection().cursor() as curs:
                curs.execute(sql_query)
            conn.connection().commit()

        sql_query = f"""
            insert into boundary_surveys.combined_labels (
                geom, village, survey_no
            )
            select
                *
            from
                boundary_surveys.{village}_original_labelled
            ;
        """
        with conn.connection().cursor() as curs:
            curs.execute(sql_query)
        conn.connection().commit()


    def label_midline_boundaries(self,village,adj_list):

        adj = []
        for i in adj_list:
            if i[0]==village :
                adj.append(i[1])
            elif i[1]==village :
                adj.append(i[0])                

        # print(f"for {village} the adjacent villages are:")
        # print(adj)
        sql_query = f"""
            -- extract the boundary narrow surveys
            -- get all the boundary surveys
            drop table if exists boundary_surveys.{village}_boundary_surveys;
            create table boundary_surveys.{village}_boundary_surveys as
            select
                faces.*
            from
                adjacent_villages.{village}_faces as faces,
                adjacent_villages.{village}_boundary as vb
            where
                st_intersects(faces.geom, st_exteriorring(vb.geom))
            ;

            -- get all the narrow faces at boundary
            drop table if exists boundary_surveys.boundary_narrows;
            create table boundary_surveys.boundary_narrows as
            select
                *
            from
                boundary_surveys.{village}_boundary_surveys
            where
                survey_no = 'रस्ता'
                or
                left(survey_no,1) = 'S'
            ;

            -- create table for village for storing the boundary strings
            drop table if exists boundary_surveys.{village}_labelled_boundary;
            create table boundary_surveys.{village}_labelled_boundary (
                geom Geometry(LineString, 32643),
                survey_no varchar(20),
                village1 varchar(200),
                village2 varchar(200)
            );

            -- get all the boundary surveys
            drop table if exists boundary_surveys.boundary_surveys;
            create table boundary_surveys.boundary_surveys as
            select
                faces.*
            from
                adjacent_villages.{village}_faces as faces,
                adjacent_villages.{village}_boundary as vb
            where
                st_intersects(faces.geom, st_exteriorring(vb.geom))
            ;

            -- get all the narrow faces at boundary
            drop table if exists boundary_surveys.boundary_narrows;
            create table boundary_surveys.boundary_narrows as
            select
                *
            from
                boundary_surveys.{village}_boundary_surveys
            where
                survey_no = 'रस्ता'
                or
                left(survey_no,1) = 'S'
            ;
        """
        with conn.connection().cursor() as curs:
            curs.execute(sql_query)
        conn.connection().commit()

        # print(village)
        for vil in adj:
            # print(f"first village for {village} is:")
            two_vills = []
            two_vills.append(vil)
            two_vills.append(village)
            two_vills.sort()
            # print(two_vills)
            # 1. get narrow strings on vil-village border
            sql_query = f"""
                -- create a temporary table for storing narrow strings
                -- after projecting on midlines
                drop table if exists boundary_surveys.temp;
                create table boundary_surveys.temp (
                    geom Geometry(LineString, 32643),
                    survey_no varchar(20),
                    village1 varchar(200),
                    village2 varchar(200)
                );

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
                    village1 = '{two_vills[0]}'
                    and
                    village2 = '{two_vills[1]}'
                ;

                -- extract the relevant boundary line of village
                drop table if exists boundary_surveys.boundary_line;
                create table boundary_surveys.boundary_line as
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
                		algo_test.{village}_split as line
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

                -- get the strings 
                drop table if exists boundary_surveys.boundary_narrow_strings;
                create table boundary_surveys.boundary_narrow_strings as
                select
                    row_number() over(order by (select NULL)) as sr_no,
                    st_intersection(
                        vb.geom,
                        st_buffer(narrow_b.geom, 0.01, 'endcap=flat join=round')
                    ) as geom,
                    survey_no
                from
                    boundary_surveys.boundary_line as vb,
                    boundary_surveys.boundary_narrows as narrow_b
                ;
            """
            # 2. separate the narrow strings
            # 3. for each separated narrow string
            #    project this string on midline of vil-village
            #    border & then label appropriately as
            #    narrow and plots
            with conn.connection().cursor() as curs:
                curs.execute(sql_query)
            conn.connection().commit()
            
            sql_query = f"""
                select
                    *
                from
                    boundary_surveys.boundary_narrow_strings
                ;
            """
            with conn.connection().cursor() as curs:
                curs.execute(sql_query)
                narrow_strings = curs.fetchall()
            conn.connection().commit()
            # print("following are narrow_strings")
            # print(narrow_strings)
            if (narrow_strings == []):
                print("-----NO NARROW STRING----")
                sql_query = f"""
                    insert into boundary_surveys.{village}_labelled_boundary (
                        geom, survey_no, village1, village2
                    )
                    select
                        vb.geom,
                        'plot' as survey_no,
                        vb.village1,
                        vb.village2
                    from
                        boundary_surveys.trial_boundary as vb
                    ;
                """
                with conn.connection().cursor() as curs:
                    curs.execute(sql_query)
                conn.connection().commit()


            for narrow_str in narrow_strings :
                # print("Hello")
                sql_query = f"""
                    ALTER TABLE boundary_surveys.boundary_narrow_strings
                      ALTER COLUMN geom 
                      TYPE Geometry(LineString,32643) 
                      USING ST_GeometryN(geom,1)
                    ;

                    drop table if exists boundary_surveys.points_to_project;
                    create table boundary_surveys.points_to_project (
                        geom Geometry(Point, 32643),
                        survey_no varchar(30)
                    );
                    insert into boundary_surveys.points_to_project (geom, survey_no)
                    select
                        st_startpoint(str.geom) as geom,
                        '{narrow_str[2]}' as survey_no
                    from
                        boundary_surveys.boundary_narrow_strings as str
                    where
                        sr_no = {narrow_str[0]}
                    ;
                """
                with conn.connection().cursor() as curs:
                    curs.execute(sql_query)
                conn.connection().commit()

                sql_query = f"""
                    insert into boundary_surveys.points_to_project (geom, survey_no)
                    select
                        st_endpoint(str.geom) as geom,
                        '{narrow_str[2]}' as survey_no
                    from
                        boundary_surveys.boundary_narrow_strings as str
                    where
                        sr_no = {narrow_str[0]}
                    ;

                """
                with conn.connection().cursor() as curs:
                    curs.execute(sql_query)
                conn.connection().commit()

                sql_query = f"""
                    -- insert the extracted narrows in the temp table
                    insert into boundary_surveys.temp (
                        geom, survey_no, village1, village2
                    )
                    select
                        st_linesubstring(
                            line.geom,
                            min(
                                st_linelocatepoint(line.geom, points.geom)
                            ),
                            max(
                                st_linelocatepoint(line.geom, points.geom)
                            )
                        ) as geom,
                        '{narrow_str[2]}' as survey_no,
                        '{two_vills[0]}' as village1,
                        '{two_vills[1]}' as village2
                    from
                        boundary_surveys.trial_boundary as line,
                        boundary_surveys.points_to_project as points
                    group by
                        line.geom
                    ;
                """
                with conn.connection().cursor() as curs:
                    curs.execute(sql_query)
                conn.connection().commit()


                # break
            sql_query = f"""
                insert into boundary_surveys.{village}_labelled_boundary (
                    geom, survey_no, village1, village2
                )
                select
                    *
                from
                    boundary_surveys.temp
                ;

                insert into boundary_surveys.{village}_labelled_boundary(
                    geom, survey_no, village1, village2
                )
                with blade as(
                    select st_buffer(geom,0.01) as geom
                    from boundary_surveys.temp as narrows
                )
                select
                    (st_dump(st_linemerge(st_difference(st_union(line.geom),st_collect(blade.geom))))).geom as geom,
                    'plot' as survey_no,
                    '{two_vills[0]}' as village1,
                    '{two_vills[1]}' as village2
                from
                    boundary_surveys.trial_boundary as line,
                    blade
                ;
            """
            with conn.connection().cursor() as curs:
                curs.execute(sql_query)
            conn.connection().commit()


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

    create_primal = labelVB()
    create_primal.run()