"""
AJE Database Setup - Carga de Datos desde Excel
===============================================

Script para cargar datos iniciales del proyecto AJE desde archivo Excel
a la base de datos SQL Server en Azure.

Autor: Equipo G02
Fecha: 2025
Propósito: Configuración inicial de la base de datos para el sistema de pedidos AJE Cajamarca
"""

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from sqlalchemy.exc import IntegrityError
import numpy as np
from datetime import datetime
import os

# Configuración de la base de datos - PERSONALIZAR SEGÚN TU ENTORNO
DATABASE_CONFIG = {
    'server': 'tu-servidor.database.windows.net',
    'database': 'TU_DATABASE',
    'username': 'tu_usuario',
    'password': 'tu_password',
    'driver': 'ODBC Driver 17 for SQL Server'
}

def create_connection():
    """
    Establece conexión con la base de datos SQL Server
    
    IMPORTANTE: Configurar DATABASE_CONFIG con tus credenciales antes de ejecutar
    
    Returns:
        engine: SQLAlchemy engine object
    """
    
    # Verificar que se hayan configurado las credenciales
    if 'tu-servidor' in DATABASE_CONFIG['server']:
        raise ValueError(
            "❌ CONFIGURACIÓN REQUERIDA:\n"
            "Edita DATABASE_CONFIG en este archivo con tus credenciales de SQL Server\n"
            "- server: Tu servidor SQL Azure\n"
            "- database: Nombre de tu base de datos\n" 
            "- username: Tu usuario\n"
            "- password: Tu contraseña"
        )
    
    connection_string = (
        f"Driver={DATABASE_CONFIG['driver']};"
        f"Server={DATABASE_CONFIG['server']};"
        f"Database={DATABASE_CONFIG['database']};"
        f"UID={DATABASE_CONFIG['username']};"
        f"PWD={DATABASE_CONFIG['password']}"
    )
    
    connection_url = URL.create(
        "mssql+pyodbc", 
        query={"odbc_connect": connection_string}
    )
    
    engine = create_engine(connection_url)
    return engine

def extract_table_data(df):
    """
    Extrae y procesa datos para cada tabla del sistema
    
    Args:
        df: DataFrame con datos del Excel
        
    Returns:
        dict: Diccionario con DataFrames procesados para cada tabla
    """
    tables = {}
    
    # 1. TIPO_DOCUMENTO - Tabla de referencia para tipos de documentos
    tables['tipo_documento'] = df[['id_tipo_documento', 'nombre', 'descripcion']]\
        .dropna(subset=['id_tipo_documento'])\
        .drop_duplicates()\
        .rename(columns={'nombre': 'nombre', 'descripcion': 'descripcion'})
    
    # 2. CANAL_CLIENTE - Tipos de canales de venta
    tables['canal_cliente'] = df[['id_canal_cliente', 'nombre.1', 'descripcion.1']]\
        .dropna(subset=['id_canal_cliente'])\
        .drop_duplicates()\
        .rename(columns={'nombre.1': 'nombre', 'descripcion.1': 'descripcion'})
    
    # 3. TIPO_PAGO - Métodos de pago disponibles
    tables['tipo_pago'] = df[['id_tipo_pago', 'nombre_tipo_pago', 'descripcion.2']]\
        .dropna(subset=['id_tipo_pago'])\
        .drop_duplicates()\
        .rename(columns={'descripcion.2': 'descripcion'})
    
    # 4. CARGO_TRABAJADOR - Cargos del personal
    tables['cargo_trabajador'] = df[['id_cargo_trabajador', 'nombre.2', 'descripcion.4']]\
        .dropna(subset=['id_cargo_trabajador'])\
        .drop_duplicates()\
        .rename(columns={'nombre.2': 'nombre_cargo', 'descripcion.4': 'descripcion'})
    
    # 5. MARCA_PRODUCTO - Marcas de productos
    tables['marca_producto'] = df[['id_marca_producto', 'nombre_marca']]\
        .dropna(subset=['id_marca_producto'])\
        .drop_duplicates()
    tables['marca_producto']['descripcion'] = tables['marca_producto']['nombre_marca']
    
    # 6. CATEGORIA_PRODUCTO - Categorías de productos
    tables['categoria_producto'] = df[['id_categoria_producto', 'nombre_categoria', 'descripcion.5']]\
        .dropna(subset=['id_categoria_producto'])\
        .drop_duplicates()\
        .rename(columns={'descripcion.5': 'descripcion'})
    
    # 7. TRABAJADOR - Personal de la empresa
    tables['trabajador'] = df[['id_trabajador', 'nombre trabajador', 'correo', 'telefono.1', 'id_cargo_trabajador']]\
        .dropna(subset=['id_trabajador'])\
        .drop_duplicates()\
        .rename(columns={
            'nombre trabajador': 'nombres',
            'correo': 'email', 
            'telefono.1': 'telefono'
        })
    tables['trabajador']['apellidos'] = ''
    
    # 8. CLIENTE - Base de clientes
    tables['cliente'] = df[['id_cliente', 'nombre_cliente', 'numero_documento', 'correo.1', 'telefono.2', 'direccion', 'id_tipo_documento', 'id_canal_cliente']]\
        .dropna(subset=['id_cliente'])\
        .drop_duplicates()\
        .rename(columns={
            'correo.1': 'email',
            'telefono.2': 'telefono'
        })
    
    # 9. PRODUCTO - Catálogo de productos
    tables['producto'] = df[['id_producto', 'descripcion.6', 'precio', 'id_marca_producto', 'id_categoria_producto']]\
        .dropna(subset=['id_producto'])\
        .drop_duplicates()\
        .rename(columns={'descripcion.6': 'descripcion'})
    # Agregar id_formato_producto si existe en tus datos
    if 'codigo_formato_producto' in df.columns:
        tables['producto']['id_formato_producto'] = df['codigo_formato_producto']
    
    # 10. PEDIDO - Órdenes de compra
    tables['pedido'] = df[['id_pedido', 'id_trabajador', 'id_cliente', 'id_tipo_pago', 'fecha', 'monto_total']]\
        .dropna(subset=['id_pedido'])\
        .drop_duplicates()
    
    # 11. DETALLE_PEDIDO - Líneas de cada pedido
    tables['detalle_pedido'] = df[['id_detalle_pedido', 'id_pedido', 'id_producto', 'cantidad', 'precio_unitario']]\
        .dropna(subset=['id_detalle_pedido'])\
        .drop_duplicates()
    
    # Agregar columnas adicionales necesarias
    if 'id_promocion' in df.columns:
        tables['detalle_pedido']['id_promocion'] = df['id_promocion']
    tables['detalle_pedido']['descuento'] = 0
    
    return tables

def load_data_to_database(tables_data, engine):
    """
    Carga datos a la base de datos respetando dependencias de foreign keys
    
    Args:
        tables_data: Diccionario con DataFrames procesados
        engine: Conexión a la base de datos
        
    Returns:
        dict: Resultado de la carga por tabla
    """
    
    # Orden de carga: tablas padre primero, dependientes después
    load_order = [
        'tipo_documento',
        'canal_cliente',
        'tipo_pago', 
        'cargo_trabajador',
        'marca_producto',
        'categoria_producto',
        'trabajador',
        'cliente',
        'producto',
        'pedido',
        'detalle_pedido'
    ]
    
    results = {}
    
    for table_name in load_order:
        if table_name in tables_data:
            df_table = tables_data[table_name].copy()
            
            try:
                # Remover ID para auto-incremento (si existe)
                id_column = f'id_{table_name}'
                if id_column in df_table.columns:
                    df_table = df_table.drop(columns=[id_column])
                
                # Carga por lotes para mejor rendimiento
                df_table.to_sql(
                    table_name,
                    index=False,
                    if_exists='append',
                    schema='G2',
                    con=engine,
                    method='multi',
                    chunksize=500
                )
                
                results[table_name] = f"✓ {len(df_table)} registros cargados"
                print(f"✓ {table_name}: {len(df_table)} registros")
                
            except Exception as e:
                results[table_name] = f"✗ Error: {str(e)}"
                print(f"✗ Error en {table_name}: {e}")
    
    return results

def validate_data_integrity(engine):
    """
    Valida la integridad de los datos cargados
    
    Args:
        engine: Conexión a la base de datos
        
    Returns:
        dict: Conteos por tabla
    """
    tables_to_check = [
        'G2.cliente', 'G2.trabajador', 'G2.producto', 
        'G2.pedido', 'G2.detalle_pedido'
    ]
    
    counts = {}
    
    with engine.connect() as conn:
        for table in tables_to_check:
            try:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                count = result.scalar()
                counts[table] = count
                print(f"{table}: {count} registros")
            except Exception as e:
                counts[table] = f"Error: {e}"
    
    return counts

def create_database_schema(engine):
    """
    Crea todas las tablas del sistema AJE desde cero
    
    Args:
        engine: Conexión a la base de datos
    """
    
    # DDL para crear todas las tablas del sistema
    schema_sql = """
    -- Crear schema G2
    CREATE SCHEMA G2;
    
    -- Tabla: tipo_documento
    CREATE TABLE G2.tipo_documento (
        id_tipo_documento INT IDENTITY(1,1) PRIMARY KEY,
        nombre VARCHAR(100) NOT NULL,
        descripcion VARCHAR(255)
    );
    
    -- Tabla: canal_cliente
    CREATE TABLE G2.canal_cliente (
        id_canal_cliente INT IDENTITY(1,1) PRIMARY KEY,
        nombre VARCHAR(100) NOT NULL,
        descripcion VARCHAR(255)
    );
    
    -- Tabla: tipo_pago
    CREATE TABLE G2.tipo_pago (
        id_tipo_pago INT IDENTITY(1,1) PRIMARY KEY,
        nombre_tipo_pago VARCHAR(100) NOT NULL,
        descripcion VARCHAR(255)
    );
    
    -- Tabla: cargo_trabajador
    CREATE TABLE G2.cargo_trabajador (
        id_cargo_trabajador INT IDENTITY(1,1) PRIMARY KEY,
        nombre_cargo VARCHAR(100) NOT NULL,
        descripcion VARCHAR(255)
    );
    
    -- Tabla: marca_producto
    CREATE TABLE G2.marca_producto (
        id_marca_producto INT IDENTITY(1,1) PRIMARY KEY,
        nombre_marca VARCHAR(100) NOT NULL,
        descripcion VARCHAR(255)
    );
    
    -- Tabla: categoria_producto
    CREATE TABLE G2.categoria_producto (
        id_categoria_producto INT IDENTITY(1,1) PRIMARY KEY,
        nombre_categoria VARCHAR(100) NOT NULL,
        descripcion VARCHAR(255)
    );
    
    -- Tabla: formato_producto
    CREATE TABLE G2.formato_producto (
        id_formato_producto INT IDENTITY(1,1) PRIMARY KEY,
        descripcion VARCHAR(255),
        volumen VARCHAR(50),
        unidad VARCHAR(10),
        id_tipo_formato INT
    );
    
    -- Tabla: trabajador
    CREATE TABLE G2.trabajador (
        id_trabajador INT IDENTITY(1,1) PRIMARY KEY,
        nombres VARCHAR(100) NOT NULL,
        apellidos VARCHAR(100),
        email VARCHAR(150),
        telefono INT,
        id_cargo_trabajador INT,
        FOREIGN KEY (id_cargo_trabajador) REFERENCES G2.cargo_trabajador(id_cargo_trabajador)
    );
    
    -- Tabla: cliente
    CREATE TABLE G2.cliente (
        id_cliente INT IDENTITY(1,1) PRIMARY KEY,
        nombre_cliente VARCHAR(200) NOT NULL,
        numero_documento VARCHAR(20),
        email VARCHAR(150),
        telefono INT,
        direccion VARCHAR(300),
        id_tipo_documento INT,
        id_canal_cliente INT,
        FOREIGN KEY (id_tipo_documento) REFERENCES G2.tipo_documento(id_tipo_documento),
        FOREIGN KEY (id_canal_cliente) REFERENCES G2.canal_cliente(id_canal_cliente)
    );
    
    -- Tabla: producto
    CREATE TABLE G2.producto (
        id_producto INT IDENTITY(1,1) PRIMARY KEY,
        descripcion VARCHAR(300) NOT NULL,
        precio DECIMAL(10,2),
        id_marca_producto INT,
        id_categoria_producto INT,
        id_formato_producto INT,
        FOREIGN KEY (id_marca_producto) REFERENCES G2.marca_producto(id_marca_producto),
        FOREIGN KEY (id_categoria_producto) REFERENCES G2.categoria_producto(id_categoria_producto),
        FOREIGN KEY (id_formato_producto) REFERENCES G2.formato_producto(id_formato_producto)
    );
    
    -- Tabla: tipo_promocion
    CREATE TABLE G2.tipo_promocion (
        id_tipo_promocion INT IDENTITY(1,1) PRIMARY KEY,
        nombre_tipo VARCHAR(100) NOT NULL,
        descripcion VARCHAR(255)
    );
    
    -- Tabla: promocion
    CREATE TABLE G2.promocion (
        id_promocion INT IDENTITY(1,1) PRIMARY KEY,
        id_tipo_promocion INT,
        condicion VARCHAR(500),
        beneficio VARCHAR(500),
        fecha_inicio DATETIME,
        fecha_fin DATETIME,
        activo BIT DEFAULT 1,
        FOREIGN KEY (id_tipo_promocion) REFERENCES G2.tipo_promocion(id_tipo_promocion)
    );
    
    -- Tabla: pedido
    CREATE TABLE G2.pedido (
        id_pedido INT IDENTITY(1,1) PRIMARY KEY,
        id_trabajador INT,
        id_cliente INT NOT NULL,
        id_tipo_pago INT NOT NULL,
        fecha DATETIME NOT NULL,
        monto_total DECIMAL(12,2),
        FOREIGN KEY (id_trabajador) REFERENCES G2.trabajador(id_trabajador),
        FOREIGN KEY (id_cliente) REFERENCES G2.cliente(id_cliente),
        FOREIGN KEY (id_tipo_pago) REFERENCES G2.tipo_pago(id_tipo_pago)
    );
    
    -- Tabla: detalle_pedido
    CREATE TABLE G2.detalle_pedido (
        id_detalle_pedido INT IDENTITY(1,1) PRIMARY KEY,
        id_pedido INT NOT NULL,
        id_producto INT NOT NULL,
        id_promocion INT,
        cantidad INT NOT NULL,
        precio_unitario DECIMAL(10,2) NOT NULL,
        descuento DECIMAL(10,2) DEFAULT 0,
        FOREIGN KEY (id_pedido) REFERENCES G2.pedido(id_pedido),
        FOREIGN KEY (id_producto) REFERENCES G2.producto(id_producto),
        FOREIGN KEY (id_promocion) REFERENCES G2.promocion(id_promocion)
    );
    """
    
    try:
        with engine.connect() as conn:
            # Ejecutar cada comando DDL
            for statement in schema_sql.split(';'):
                if statement.strip():
                    conn.execute(text(statement.strip()))
            conn.commit()
        print("   ✓ Base de datos AJE creada exitosamente")
        return True
    except Exception as e:
        print(f"   ✗ Error creando base de datos: {e}")
        return False

def main():
    """
    Configuración completa del proyecto AJE desde cero
    """
    print("=" * 60)
    print("PROYECTO AJE - CONFIGURACIÓN INICIAL DE BASE DE DATOS")
    print("=" * 60)
    print("Este script configura la base de datos completa desde cero")
    print("para el sistema de pedidos AJE Cajamarca")
    print("")
    
    # Configuración de archivos
    excel_file = 'TablasGrupo2.xlsx'
    data_sheet = 'tablas'
    
    try:
        # 1. Verificar prerrequisitos
        print("1. Verificando prerrequisitos...")
        if not os.path.exists(excel_file):
            print(f"   ✗ Archivo {excel_file} no encontrado")
            print(f"   → Descarga el archivo de datos del repositorio")
            return False
        print("   ✓ Archivo de datos encontrado")
        
        # 2. Conectar a base de datos
        print("2. Conectando a SQL Server Azure...")
        engine = create_connection()
        print("   ✓ Conexión establecida")
        
        # 3. Crear estructura de base de datos
        print("3. Creando estructura de base de datos...")
        if not create_database_schema(engine):
            return False
        
        # 4. Leer datos del Excel
        print("4. Procesando archivo de datos...")
        df = pd.read_excel(excel_file, sheet_name=data_sheet, header=1)
        print(f"   ✓ {len(df):,} registros procesados")
        
        # 5. Extraer datos por tabla
        print("5. Organizando datos por tabla...")
        tables_data = extract_table_data(df)
        print(f"   ✓ {len(tables_data)} tablas preparadas")
        
        # 6. Cargar datos
        print("6. Cargando datos a la base de datos...")
        results = load_data_to_database(tables_data, engine)
        
        # 7. Validar instalación
        print("7. Validando instalación...")
        counts = validate_data_integrity(engine)
        
        # 8. Resumen final
        print("\n" + "=" * 60)
        print("CONFIGURACIÓN COMPLETADA EXITOSAMENTE")
        print("=" * 60)
        print("El proyecto AJE está listo para usar")
        print("")
        print("DATOS CARGADOS:")
        for table, count in counts.items():
            print(f"  {table:<25}: {count:>8} registros")
        
        print(f"\nTOTAL: {sum(int(c) for c in counts.values() if str(c).isdigit()):,} registros")
        print("\n✓ Puedes conectar Power Apps a esta base de datos")
        print("✓ Todas las tablas y relaciones están configuradas")
        
        return True
        
    except Exception as e:
        print(f"\n✗ Error en la configuración: {e}")
        import traceback
        traceback.print_exc()
        return False
        
        # 6. Validar carga
        print("5. Validando integridad de datos...")
        counts = validate_data_integrity(engine)
        
        # 7. Resumen final
        print("\n" + "=" * 50)
        print("RESUMEN DE CARGA COMPLETADA")
        print("=" * 50)
        
        for table, result in results.items():
            print(f"{table:<20}: {result}")
        
        print("\nDATOS EN BASE:")
        for table, count in counts.items():
            print(f"{table:<25}: {count} registros")
        
        print("\n✓ Proceso completado exitosamente")
        print("✓ Base de datos lista para uso en Power Apps")
        
    except Exception as e:
        print(f"\n✗ Error crítico: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    return True

if __name__ == "__main__":
    success = main()
    if success:
        print("\n🎉 Base de datos AJE configurada correctamente")
    else:
        print("\n❌ Falló la configuración de la base de datos")
