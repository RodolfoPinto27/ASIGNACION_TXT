import pyodbc
import pandas as pd
import os
import glob
from datetime import datetime
import time

# Configuración de conexión
nombre_servidor = "180.194.16.194"
nombre_base_datos = "GestionCobranza"
connection_string = f"DRIVER={{SQL Server}};SERVER={nombre_servidor};DATABASE={nombre_base_datos};Trusted_Connection=yes;"

# Lista de empresas
empresas = [
    "COBINTEL", "CR", "EXTERNA", "INCOBRO", "KOBSA",
    "MLR", "COBRANZAS_PERU", "NEO", "VERTICE"
]

# Configuración de rutas
ruta_base = "C:\\DATOS\\Reportes\\Asignacion\\Archivos"
os.makedirs(ruta_base, exist_ok=True)

fecha_carga = datetime.now().strftime("%Y%m%d")

def limpiar_carpeta(ruta):
    archivos = glob.glob(f"{ruta}\\*_{fecha_carga}.txt")
    for archivo in archivos:
        os.remove(archivo)

def generar_archivos_txt():
    tiempo_inicio_total = time.time()
    
    # Conexión a la base de datos
    with pyodbc.connect(connection_string) as conn:
        for empresa in empresas:
            tiempo_inicio_empresa = time.time()
            ruta_empresa = os.path.join(ruta_base, empresa)
            os.makedirs(ruta_empresa, exist_ok=True)
            
            # Limpiar archivos del día
            limpiar_carpeta(ruta_empresa)
            
            # Consultas para cada pestaña
            queries = {
                "ASIGNACION": f"EXEC [dbo].[USP_PESTANIA_ASIGNACION] '{empresa}'",
                "CLIENTE": f"EXEC [dbo].[USP_PESTANIA_CLIENTE] '{empresa}'",
                "DIRECCION": f"EXEC [dbo].[USP_PESTANIA_DIRECCION] '{empresa}'",
                "TELEFONOS": f"EXEC [dbo].[USP_PESTANIA_TELEFONO] '{empresa}'",
                "BLACKLIST": "SELECT * FROM UTB_FORMATO_BLACKLIST",
                "PAGOS": "SELECT * FROM UTB_FORMATO_PAGOS ORDER BY IDENT_OPERACION, FECHA_PAGO, CUOTA",
                "INICIO": f"EXEC [dbo].[USP_PESTANIA_INDICADORES_ASIG] '{empresa}'"
            }
            
            print(f"\nProcesando empresa: {empresa}")
            for tabla, query in queries.items():
                df = pd.read_sql(query, conn)
                
                # Renombrar columnas según el archivo TXT
                if tabla == "ASIGNACION":
                    df.columns = [
                        "FECHA_ASIGNACION", "ID_CLIENTE", "OPERACION", "DOCUMENTO", "NRO_CUOTA",
                        "FECHA_VENCIMIENTO", "MONEDA", "SALDO_OPERACION", "CAPITAL", "MONTO_INTERES",
                        "SEG_VEHICULAR", "SEG_DESGRAVAMEN", "MONTO_PORTE", "INTERES_MORATORIO", "CUOTA",
                        "MONTO_TOTAL", "ESTADO", "CCI", "PRODUCTO", "NRO CUOTAS PAGADAS", "TOTAL PLAZO",
                        "DIAS MORA", "FECHA ULT PAGO", "SOBREENDEUDADO", "MARCA", "MODELO", "RANGO_CAPITAL",
                        "NRO_ENTIDADES_SSFF", "CUOTAS_DOBLES", "INGRESO", "EMPRESA_GESTION", "EMPRESA_CAMPO",
                        "EMPRESA_BACKUP", "SALDO_CAPITAL", "GPS", "CESANTIA", "HABITO", "TRAMO DE MORA",
                        "DELTA", "REPROGRAMADO", "REPROGRAMADO_CUOTA", "CAMPANIA", "MONTO_CAMPANIA",
                        "TIPO_CLIENTE", "CLASIFICACION_SCP", "CLASIFICACION_RESULTANTE", "TRAMO_OPERACION_REAL",
                        "FLAG GESTION", "TIP RES", "TIP RES ANT", "GARANTIA_VEHICULAR", "INCAUTADO",
                        "DEVUELTO", "FECHA_INCAUTADO", "FECHA_VENTA", "FLG_JUDICIAL", "TIPO_CLIENTE_4",
                        "MEJOR GESTION ACTUAL", "ESTUDIO PDP ACTUAL", "FECHA PDP ACTUAL", "MONTO PDP ACTUAL",
                        "MEJOR GESTION PREVIO", "MEJOR GESTION HIST", "ULT FECHA PDP", "GESTION SCP",
                        "TEA", "TCEA", "CATEGORIA_LABORAL", "RUBRO", "DEUDA TOTAL", "CANAL GESTION PERMITIDO",
                        "ESTRATEGIA", "SPEECH ESTRATEGIA", "TABLA", "NUEVA_ASIGNACION", "NODO_RIESGO_R",
                        "NODO_RIESGO_G", "GESTOR INTERNO", "FECHA REGISTRO OCD", "GASTOS JUDICIALES",
                        "MEJOR_CDI_DIGITAL_3M", "FLAG WHATSAPP", "NODO_BM_G05_T04", "NODO_BM_R15",
                        "EMPRESA NEGOCIACION INCAUTADO", "PILOTO LTV", "CAMPAÑA PONTE AL DIA",
                        "PORC_DSCTO_CAPITAL", "SEGMENTO_NEW", "ORIGEN CUENTA", "FLG_VENTA_CARTERA",
                        "FLG_MIGRACION", "FLG_INSCRITO", "VALOR COMERCIAL", "MAYOR OFERTA"
                    ]
                elif tabla == "PAGOS":
                    df.columns = [
                        "FECHA PAGO", "FECHA VENCIMIENTO", "CUOTA", "OPERACION", "MONEDA", "TOTAL CUOTA"
                    ]
                elif tabla == "BLACKLIST":
                    df.columns = [
                        "FECHA PROCESO", "ID CLIENTE", "DOCUMENTO", "TELEFONO", "DETALLE"
                    ]
                elif tabla == "TELEFONOS":
                    df.columns = [
                        "ID_CLIENTE", "DOCUMENTO", "TIPO", "TELEFONO", "ANEXO", "CANAL"
                    ]
                elif tabla == "DIRECCION":
                    df.columns = [
                        "ID_CLIENTE", "DOCUMENTO", "EMAIL TITULAR", "EMAIL_CONYUGE", "DIRECCION DOMICILIO",
                        "DEPARTAMENTO", "PROVINCIA", "DISTRITO", "TIPO", "AGENCIA"
                    ]
                elif tabla == "CLIENTE":
                    df.columns = [
                        "ID_CLIENTE", "NOMBRE TITULAR", "TIPO DOCUMENTO TITULAR", "NRO DOCUMENTO TITULAR",
                        "FECHA NACIMIENTO", "NOMBRE CONYUGE", "TIPO DOCUMENTO CONYUGE", "NRO DOCUMENTO CONYUGE"
                    ]
                elif tabla == "INICIO":
                    df.columns = [
                        "PERIODO ASIGNACION", "ID CLIENTE", "TRAMO", "PRODUCTO", "MTO_SALDO_MN", 
                        "MTO_SALDO_MN_PAS", "MTO_REQUERIDO_MN", "RECUPERO_ACOTADO", "MTO_PAGO_MN", 
                        "TIP_RES", "TIPO_CLIENTE_8_AJU", "EMPRESA", "EMPRESA_BACKUP", "EMPRESA_CAMPO", 
                        "INTERNO", "EMPRESA_JJC", "EMPRESA_ESPECIAL", "EMPRESA_BOTS", 
                        "EMPRESA_REPUBLICADOS", "TIPO_CIENTE_9_S", "EMPRESA NEGOCIACION INCAUTADO", 
                        "ORIGEN CUENTA"
                    ]
                
                # Reemplazar NULL por cadena vacía
                df.fillna('', inplace=True)
                
                # Guardar como TXT con cabeceras
                file_name = f"ASIGNACION_{empresa}_{tabla}_{fecha_carga}.txt"
                file_path = os.path.join(ruta_empresa, file_name)
                df.to_csv(file_path, sep='\t', index=False, header=True)
                print(file_name)

            tiempo_fin_empresa = time.time()
            print(f"{empresa} TIEMPO PROCESO TXT: {tiempo_fin_empresa - tiempo_inicio_empresa:.2f} segundos")

    tiempo_fin_total = time.time()
    tiempo_total_minutos = (tiempo_fin_total - tiempo_inicio_total) / 60
    print(f"\nCARGA LISTA EN: {tiempo_total_minutos:.2f} minutos")

# Generar archivos TXT
generar_archivos_txt()







 







