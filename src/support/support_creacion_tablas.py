# database agent
import psycopg2
from psycopg2 import OperationalError, errorcodes, errors

# data processing
import pandas as pd

# system
import os

# functions typing
from typing import Optional, Tuple, List, Union, Dict


def drop_all_tables(conn: psycopg2.extensions.connection) -> None:
    """
    Drops all tables from the database with CASCADE.

    Parameters:
    ----------
    conn : psycopg2.extensions.connection
        Connection to the PostgreSQL database.
    """
    with conn.cursor() as cursor:
        cursor.execute(
            "DROP TABLE IF EXISTS centro_hospitalario, tipos_hospitalizacion, gastos, ingresos CASCADE;"
        )
        conn.commit()



def create_all_tables(conn: psycopg2.extensions.connection) -> None:
    """
    Creates all tables in the database by calling specific creation functions.

    Parameters:
    ----------
    conn : psycopg2.extensions.connection
        Connection to the PostgreSQL database.
    """
    crear_centro_hospitalario(conn)
    crear_tipos_hospitalizacion(conn)
    crear_gastos(conn)
    crear_ingresos(conn)



def crear_centro_hospitalario(conn: psycopg2.extensions.connection) -> None:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            CREATE TABLE centros_hospitalarios(
                ncodi INT PRIMARY KEY,
                nombre_centro VARCHAR(50)
            );
            """
        )
        conn.commit()

def crear_tipos_hospitalizacion(conn: psycopg2.extensions.connection) -> None:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            CREATE TABLE tipos_hospitalizacion(
                id_hospitalizacion SERIAL PRIMARY KEY
            )
            """
        )
        conn.commit()


def crear_gastos(conn: psycopg2.extensions.connection) -> None:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            CREATE TABLE gastos(
                id_gasto SERIAL PRIMARY KEY,
                anio INT NOT NULL,
                ncodi INT REFERENCES centro_hospitalario(ncodi),
                tipo_gasto VARCHAR(50) NOT NULL,
                monto NUMERIC CHECK(monto >= 0)
                
            )
            """
        )
        conn.commit()


def crear_ingresos(conn: psycopg2.extensions.connection) -> None:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            CREATE TABLE ingresos(
                id_ingreso SERIAL PRIMARY KEY,
                anio INT NOT NULL,
                ncodi INT REFERENCES centro_hospitalario(ncodi),
                id_tipo_hospitalizacion INT REFERENCES tipo_hospitalizacion(id_tipo_hospitalizacion),
                fuentes_ingresos VARCHAR(50) NOT NULL,
                monto NUMERIC CHECK(monto >= 0)
            );
            """
        )
        conn.commit()


# def insert_centro_hospitalario(conn: psycopg2.extensions.connection, brand_name: str) -> int:
#     """
#     Inserts a new brand or returns an existing brand ID.

#     Parameters:
#     ----------
#     conn : psycopg2.extensions.connection
#         Connection to the PostgreSQL database.
#     brand_name : str
#         The name of the brand to insert or find.

#     Returns:
#     -------
#     int
#         The ID of the brand.
#     """
#     with conn.cursor() as cursor:
#         cursor.execute("SELECT brand_id FROM brands WHERE brand_name = %s", (brand_name,))
#         brand_id = cursor.fetchone()
        
#         if not brand_id:
#             cursor.execute(
#                 "INSERT INTO brands (brand_name) VALUES (%s) RETURNING brand_id",
#                 (brand_name,)
#             )
#             brand_id = cursor.fetchone()[0]
#             conn.commit()
#         else:
#             brand_id = brand_id[0]
    
#     return brand_id
