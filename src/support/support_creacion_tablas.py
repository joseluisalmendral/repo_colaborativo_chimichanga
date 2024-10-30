# database agent
import psycopg2
from psycopg2 import OperationalError, errorcodes, errors

# data processing
import pandas as pd

# system
import os

# functions typing
from typing import Optional, Tuple, List, Union, Dict



query_crear_centros_hospitalarios = """
                                    CREATE TABLE centros_hospitalarios(
                                        ncodi INT PRIMARY KEY,
                                        nombre_centro VARCHAR(50)
                                    );
                                    """

query_crear_tipos_hospitalizacion ="""
                                    CREATE TABLE tipos_hospitalizacion(
                                        id_hospitalizacion SERIAL PRIMARY KEY,
                                        desc_tipo_hospitalizacion VARCHAR(50) UNIQUE NOT NULL
                                    );
                                    """

query_crear_gastos = """
CREATE TABLE gastos(
    id_gasto SERIAL PRIMARY KEY,
    anio INT NOT NULL,
    ncodi INT REFERENCES centro_hospitalario(ncodi),
    tipo_gasto VARCHAR(50) NOT NULL,
    monto NUMERIC CHECK(monto >= 0)
    
);
"""

query_crear_ingresos = """
CREATE TABLE ingresos(
    id_ingreso SERIAL PRIMARY KEY,
    anio INT NOT NULL,
    ncodi INT REFERENCES centro_hospitalario(ncodi),
    id_tipo_hospitalizacion INT REFERENCES tipo_hospitalizacion(id_tipo_hospitalizacion),
    fuentes_ingresos VARCHAR(50) NOT NULL,
    monto NUMERIC CHECK(monto >= 0)
);
"""

# Centro hospitalario
query_creation_hospitales = """
create table if not exists hospitales (
    ncodi INT primary key,
    name VARCHAR(300)
);
"""

# Tipos
query_creation_tipo_hosp = """
create table if not exists tipo_hospitalizacion (
    tipo_id SERIAL primary key,
    nombre VARCHAR(100) unique not null
    );
"""

# Gastos
query_creation_gastos = """
create table if not exists gastos (
    gastos_id INT primary key,
    año INT not null,
    ncodi INT references hospitales(ncodi),
    totalcompra NUMERIC, 
    producfarma NUMERIC, 
    materialsani NUMERIC, 
    implantes NUMERIC, 
    restomateriasani NUMERIC, 
    servcontratado NUMERIC, 
    trabajocontratado NUMERIC, 
    xrestocompras NUMERIC, 
    variaexistencias NUMERIC, 
    servexteriores NUMERIC, 
    sumistro NUMERIC, 
    xrestoserviexter NUMERIC, 
    gastopersonal NUMERIC, 
    sueldos NUMERIC, 
    indemnizacion NUMERIC, 
    segsocempresa NUMERIC, 
    otrgassocial NUMERIC, 
    dotaamortizacion NUMERIC, 
    perdidadeterioro NUMERIC, 
    xrestogasto NUMERIC, 
    totcompragasto NUMERIC
);
"""

# Ingresos
query_creation_ingresos = """
create table if not exists ingresos (
    id_ingresos INT primary key,
    ncodi INT references hospitales(ncodi),
    particulares NUMERIC,
    aseguradoras NUMERIC,
    aseguradoras_enfermedad NUMERIC,
    aseguradoras_trafico NUMERIC,
    mutuas NUMERIC,
    tipo_id INT not null references tipo_hospitalizacion(tipo_id)
);"""




def dropear_tablas(conn: psycopg2.extensions.connection, lista_tablas: List[str]) -> None:
    """
    Drops all tables from the database with CASCADE.

    Parameters:
    ----------
        - conn (psycopg2.extensions.connection): Connection to the PostgreSQL database.
        - lista_tablas (List[str]): List of tables to drop.
    """
    
    lista_tablas_string = ", ".join(lista_tablas)

    with conn.cursor() as cursor:
        cursor.execute(f"DROP TABLE IF EXISTS {lista_tablas_string} CASCADE;"
        )
        conn.commit()



# Usando autocommit como usamos en clase, no es necesaria esta funcion, ver notebooks/creacion_tablas.ipynb
# def crear_tabla(conn: psycopg2.extensions.connection, query_creacion_tabla=str) -> None:
#     """
#     Creates a table in the database with the query taken as input.

#     Parameters:
#     ----------
#         - conn (psycopg2.extensions.connection): Connection to the PostgreSQL database.
#         - query_creacion_tabla (str): List of tables to drop.
#     """
#     with conn.cursor() as cursor:
#         cursor.execute(query_creacion_tabla)
#         conn.commit()




query_creation_hospitales = """
create table if not exists hospitales (
    ncodi INT primary key,
    name VARCHAR(300)
);
"""


def insert_centro_hospitalario(conn: psycopg2.extensions.connection, ncodi: int, name: str) -> int:
    """
    Inserts a new brand or returns an existing brand ID.

    Parameters:
    ----------
    conn : psycopg2.extensions.connection
        Connection to the PostgreSQL database.
    brand_name : str
        The name of the brand to insert or find.

    Returns:
    -------
    int
        The ID of the brand.
    """
    with conn.cursor() as cursor:
            cursor.execute(
                "INSERT INTO hospitales (ncodi,name) VALUES (%s,%s)",
                (ncodi,name)
            )
            conn.commit()
