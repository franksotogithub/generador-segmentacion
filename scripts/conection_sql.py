
import pymssql
import arcpy


#server = "172.18.1.41"

def Conexion():
    server = "172.18.1.93"
    user = "us_arcgis_seg_2"
    password = "MBs0p0rt301"
    connx = pymssql.connect(server, user, password, "CPV_SEGMENTACION")
    return connx

def Conexion2():
    server = "172.18.1.93"
    user = "sde"
    password = "$deDEs4Rr0lLo"
    connx = pymssql.connect(server, user, password, "CPV_SEGMENTACION_GDB")
    return connx

def Conexion3():
    server = '192.168.202.84'
    user = 'sde'
    password = 'wruvA7a*tat*'
    database = 'CPV_MONITOREO_GIS'
    connx = pymssql.connect(server, user, password, database)
    return connx

def Conexion4():
    server = "192.168.203.102"
    db="DIVIES2015_CONSISTENCIA"
    user = "us_cartografia_divies_ejec"
    password = "ra?haNaqa7U3"

    connx = pymssql.connect(server, user, password, db)
    return connx


def Conexion5():
    server = "192.168.10.150"
    db="DIVIES2014_BDCONSISTENCIA"
    user = "us_cartografia_divies_ejec"
    password = "ra?haNaqa7U3"

    connx = pymssql.connect(server, user, password, db)
    return connx


def actualizar_viv_2015(ubigeo,zona=''):
    conn=Conexion4()
    cursor = conn.cursor()
    sql_query="EXEC CARGA_UBIGEOSCON2015_CPV0301_URB '{ubigeo}','{zona}'  ".format(ubigeo=ubigeo,zona=zona)
    cursor.execute(sql_query)
    conn.commit()
    conn.close()

def actualizar_viv_2014(ubigeo,zona=''):
    conn=Conexion5()
    cursor = conn.cursor()
    sql_query="EXEC CARGA_UBIGEOSCON2014_CPV0301_URB '{ubigeo}','{zona}'  ".format(ubigeo=ubigeo,zona=zona)
    cursor.execute(sql_query)
    conn.commit()
    conn.close()



def conexion_arcgis(db,ip_server,usuario,password):

    if arcpy.Exists("{}.sde".format(db)) == False:
        arcpy.CreateDatabaseConnection_management("Database Connections",
                                                  "{}.sde".format(db),
                                                  "SQL_SERVER",
                                                  ip_server,
                                                  "DATABASE_AUTH",
                                                  usuario,
                                                  password,
                                                  "#",
                                                  "{}".format(db),
                                                  "#",
                                                  "#",
                                                  "#",
                                                  "#")
    path_conexion = "Database Connections/{}.sde".format(db)
    return path_conexion



def ActualizarRegistrosCPVSegmentacion(codigo,fase='CPV2017'):
    conn=Conexion()
    cursor = conn.cursor()
    sql_query = """
        exec USP_ACTUALIZAR_REGISTROS_URBANO_CPV_SEGMENTACION '{}','{}'
        """.format(codigo,fase)
    print sql_query
    cursor.execute(sql_query)
    conn.commit()
    conn.close()

def actualizar_etiq_zona():
    conn=Conexion()
    cursor = conn.cursor()
    sql_query = """ exec USP_ACTUALIZA_ETIQUETA_ZONA """
    cursor.execute(sql_query)
    conn.commit()
    conn.close()


def obtener_lista_zonas_calidad(cant_zonas):
    conn = Conexion()
    cursor = conn.cursor()


    sql_query="""
                exec USP_OBTENER_LISTA_ZONAS_CALIDAD {}
                """.format(cant_zonas)



    print sql_query

    cursor.execute(sql_query)
    data=[]
    for row in cursor:
        data.append(["{}".format(row[0]),"{}".format(row[1])])

    conn.commit()
    conn.close()
    print data
    return data



def actualizar_flag_calidad_input_zonas(ubigeo,zona,flag,equipo):
    conn = Conexion()
    cursor = conn.cursor()


    if flag==1:


        sql_query="exec USP_ACTUALIZAR_FLAG_PROC_CALIDAD '{}', '{}', '{}'".format(ubigeo,zona,equipo)
    else:
        sql_query ="""
        update a
        set a.flag_proc_calidad_input={} , a.equipo_proc_calidad_input='{}',a.fec_proc_calidad_input=getdate()
        from (select * from tb_zona where ubigeo='{}' and zona='{}' )a
        """.format(flag,equipo,ubigeo,zona)
    cursor.execute(sql_query)

    conn.commit()
    conn.close()


def actualizar_flag_proc_segm(ubigeo,zona,flag,equipo,fase,error=''):
    conn = Conexion()
    cursor = conn.cursor()

    sql_query = """
    begin
    update a
    set a.flag_proc_segm={flag},a.fec_proc_segm=GETDATE(),a.equipo_proc_segm='{equipo}',a.error='{error}'
    from marco_zona a
    where a.ubigeo='{ubigeo}' and a.zona='{zona}' and a.fase='{fase}'

    end
    """.format(ubigeo=ubigeo,zona=zona,flag=flag,equipo=equipo,fase=fase,error=error)



    print sql_query
    cursor.execute(sql_query)
    if flag==2:
        sql_query2 = """
        begin
        update a
        set a.flag_segm_u=0
        from marco_distrito a
        where a.ubigeo='{ubigeo}' and a.fase='{fase}'

        end
        """.format(ubigeo=ubigeo,  fase=fase)

        cursor.execute(sql_query2)


    conn.commit()
    conn.close()



def actualizar_flag_insert_data_segm(ubigeo,zona,flag,fase='CPV2017'):
    conn = Conexion()
    cursor = conn.cursor()

    sql_query = """
    begin
    update a
    set a.flag_data_insert='{flag}'
    from marco_zona a
    where a.ubigeo='{ubigeo}' and a.zona='{zona}' and a.fase='{fase}'

    end
    """.format(ubigeo=ubigeo,zona=zona,fase=fase,flag=flag)
    cursor.execute(sql_query)

    conn.commit()
    conn.close()



def actualizar_flag_proc_segm_prueba(ubigeo,zona,flag,equipo,fase):
    conn = Conexion()
    cursor = conn.cursor()

    sql_query = """
    begin
    update a
    set a.flag_proc_segm_prueba={flag},a.fec_proc_segm_prueba=GETDATE(),a.equipo_proc_segm_prueba='{equipo}'
    from marco_zona a
    where a.ubigeo='{ubigeo}' and a.zona='{zona}' and a.fase='{fase}'

    end
    """.format(ubigeo=ubigeo,zona=zona,flag=flag,equipo=equipo,fase=fase)

    print sql_query
    cursor.execute(sql_query)

    conn.commit()
    conn.close()





def actualizar_proc_calidad_zonas(ubigeo,zona):
    conn = Conexion()
    cursor = conn.cursor()

    sql_query = """
            begin
            update  a
            set a.flag_proc_calidad_input={}
            from tb_zona a
            inner join (select case when ( 'SI'  ) from  vw_control_calidad_input_urbano
                        where ubigeo={} and zona={} )  b on a.ubigeo=b.ubigeo and a.zona=b.zona

            end


                """.format(ubigeo,zona)
    cursor.execute(sql_query)
    data=[]

    conn.commit()
    conn.close()
    print data
    return data



def actualizar_imp_total_zona(ubigeo,zona,flag,fase='CPV2017'):
    c=Conexion()
    cursor=c.cursor()
    sql = """update b
        set b.flag_imp_total={flag}
        from dbo.MARCO_ZONA b
        where b.ubigeo='{ubigeo}' and b.zona='{zona}' and b.fase='{fase}'
    """.format(ubigeo=ubigeo, zona=zona,flag=flag,fase=fase)


    cursor.execute(sql)
    c.commit()
    c.close





def actualizar_cant_viv_mzs(data):
    conn=Conexion2()
    cursor = conn.cursor()
    for row in data:
        if len(row) ==1:
            codigo=row[0]
        else:
            codigo = '{}{}'.format(row[0],row[1])
        sql_query = """
            exec USP_ACTUALIZAR_CANT_VIV_MZS '{}'
            """.format(codigo)
        cursor.execute(sql_query)
        conn.commit()

    conn.close()




def ActualizarJefeHogar(data,tipo):
    conn=Conexion2()
    cursor = conn.cursor()
    for row in data:
        if len(row) == 1:
            zona="99999"
        elif len(row) == 2:

            zona=str(row[1])
        sql_query = """
            exec USP_ACTUALIZAR_JEFE_HOGAR '{ubigeo}', '{zona}' ,'{tipo}'
            """.format(ubigeo=str(row[0]), zona=zona, tipo=tipo)
        cursor.execute(sql_query)

        conn.commit()
    conn.close()


def obtener_lista_zonas_segmentacion(cant_zonas,fase='CPV2017'):
    conn = Conexion()
    cursor = conn.cursor()
    sql_query = """
                exec USP_OBTENER_LISTA_ZONAS_SEGMENTACION  {},'{}'
                """.format(cant_zonas,fase)
    print sql_query
    cursor.execute(sql_query)
    data=[]
    for row in cursor:

        data.append(["{}".format(row[0]),"{}".format(row[1]),row[2]])
    conn.commit()
    conn.close()
    return data




def obtener_lista_zonas_merge(cant_zonas,fase='CPV2017'):
    conn = Conexion()
    cursor = conn.cursor()
    sql_query = """
    declare @table TABLE(
                    ubigeo varchar(6),
                    zona varchar(6)
                    );

            insert @table
            select top ({cant_zonas}) a.ubigeo,a.zona from marco_zona a
            where
            a.fase='{fase}'  and
            isnull(a.flag_imp_total,0 ) in (0)

    order by a.ubigeo ,a.zona

    update  a
             set a.flag_imp_total=3
             from marco_zona a
             inner join @table b on a.ubigeo=b.ubigeo and a.zona=b.zona and a.fase='{fase}'

             select ubigeo,zona from @table


                """.format(cant_zonas=cant_zonas,fase=fase)
    print sql_query
    cursor.execute(sql_query)
    data=[]
    for row in cursor:
        data.append(["{}".format(row[0]),"{}".format(row[1])])
    conn.commit()
    conn.close()
    return data



def obtener_lista_zonas_segmentacion_prueba(cant_zonas,fase='CPV2017'):
    conn = Conexion()
    cursor = conn.cursor()
    sql_query = """
    declare @table TABLE(
                    ubigeo varchar(6),
                    zona varchar(6)
                    );

            insert @table
            select top ({cant_zonas}) a.ubigeo,a.zona from marco_zona a
            where
            a.fase='{fase}'  and
            isnull(a.flag_proc_segm_prueba,0 ) in (0)

    order by a.ubigeo desc,a.zona desc

    update  a
             set a.flag_proc_segm_prueba=3
             from marco_zona a
             inner join @table b on a.ubigeo=b.ubigeo and a.zona=b.zona and a.fase='{fase}'

             select ubigeo,zona from @table


                """.format(cant_zonas=cant_zonas,fase=fase)
    print sql_query
    cursor.execute(sql_query)
    data=[]
    for row in cursor:
        data.append(["{}".format(row[0]),"{}".format(row[1])])
    conn.commit()
    conn.close()
    return data

def obtener_lista_distritos(cant_zonas,fase='CPV2017'):
    conn = Conexion()
    cursor = conn.cursor()
    sql_query = """
                exec USP_OBTENER_LISTA_ZONAS_SEGMENTACION  {},'{}'
                """.format(cant_zonas,fase)
    print sql_query
    cursor.execute(sql_query)
    data=[]
    for row in cursor:
        data.append(["{}".format(row[0]),"{}".format(row[1])])
    conn.commit()
    conn.close()
    return data



def ObtenerZonasPrueba(cant_zonas):
    conn = Conexion()
    cursor = conn.cursor()
    sql_query = """
                exec USP_OBTENER_ZONAS_PRUEBA {}
                """.format(cant_zonas)
    cursor.execute(sql_query)
    data=[]
    for row in cursor:
        data.append(["{}".format(row[0]),"{}".format(row[1])])
    conn.commit()
    conn.close()
    return data


def ObtenerReporteDistrital(ubigeo,distope,fase):
    conn = Conexion()
    cursor = conn.cursor()



    if distope!='00':
        sql_query_cabecera = """
                select a.ubigeo,a.ccdd,a.nombdep ,a.ccpp,a.nombprov,a.ccdi,a.nombdist,b.nombdistope
                from
                marco_distrito a
                inner join dbo.DISTRITO_OPE b on a.ubigeo=b.ubigeo
                where b.id='{}{}'  and  b.FASE='{}' """.format(ubigeo,distope,fase)

    else:
        sql_query_cabecera = """
            select a.ubigeo,a.ccdd,a.nombdep ,a.ccpp,a.nombprov,a.ccdi,a.nombdist,a.nombdist nombdistope
            from
            marco_distrito a
            where a.ubigeo='{}'  and  a.FASE='{}' """.format(ubigeo, fase)

    cursor.execute(sql_query_cabecera)
    cabecera = []



    for row in cursor:
        cabecera=row

    sql_query_data = """
    EXEC USP_REPORTE_DISTRITAL '{}','{}','{}'
    """.format(ubigeo,distope,fase)
    cursor.execute(sql_query_data)
    data=[]
    for row in cursor:
        data.append(row)

    sql_query_resumen = """exec USP_RESUMEN_DISTRITAL '{}','{}','{}'""".format(ubigeo,distope,fase)


    cursor.execute(sql_query_resumen)
    resumen = []
    for row in cursor:
        resumen=row

    conn.commit()
    conn.close()


    return [cabecera,data,resumen]


def ObtenerReporteAEUFenomeno(ubigeo,zona,aeu):
    conn = Conexion()
    cursor = conn.cursor()

    sql_query_cabecera = """
                select a.ubigeo,a.ccdd,a.departamento,a.ccpp,a.provincia,a.ccdi,a.distrito,b.codccpp,b.nomccpp
                'CIUDAD' , C.ETIQ_ZONA, D.subzona, isnull(d.seccion,''),
                d.AEU, d.cant_viv
                from VW_UBIGEOS A
                inner join TB_CCPP B ON a.UBIGEO=B.UBIGEO
                inner join TB_ZONA C ON C.UBIGEO=b.UBIGEO  and b.codccpp=c.codccpp
                inner join segm_u_aeu D ON  d.UBIGEO=A.UBIGEO AND C.ZONA=D.ZONA
                where  D.FLAG_FENOMENO=1
                and d.UBIGEO='{}' and d.ZONA='{}'  and  cast(D.AEU as int)={}  """.format(ubigeo,zona,aeu)


    print sql_query_cabecera

    cursor.execute(sql_query_cabecera)
    cabecera = []


    for row in cursor:
        cabecera=row


    sql_query_data = """
SELECT
OR_VIV_AEU_FEN OR_VIV_AEU,A.MANZANA,a.id_reg_or ,A.FRENTE_ORD ,C.P20_NOMBRE  P20,  A.P21  P21,  A.P22_A , A.P23 , A.P24
,A.P25 , A.P26 , A.P27_A , A.P28  , isnull(A.JEFE_HOGAR,'') P32
FROM    dbo.TB_CPV0301_VIVIENDA_U A
INNER JOIN   dbo.TB_TIPO_VIA    C   ON C.P20=A.P20
WHERE a.UBIGEO='{}' AND a.ZONA='{}' AND CAST(a.AEU_FEN AS int) ={}
ORDER BY A.UBIGEO,A.ZONA,A.AEU,A.MANZANA,A.OR_VIV_AEU_FEN
""".format(ubigeo,zona,aeu)
    cursor.execute(sql_query_data)
    data=[]
    for row in cursor:
        data.append(row)

    conn.commit()
    conn.close()

    return [cabecera,data]


#print ObtenerReporteDistrital('150116')

def ActualizarCampoMzCondominio(data):
    conn = Conexion()
    cursor = conn.cursor()
    for row in data:
        if len(row) == 1:

            sql_query = """
                exec ACTUALIZAR_CAMPO_MZS_CONDOMINIO '{ubigeo}', '{zona}'
                """.format(ubigeo=str(row[0]), zona="99999")
            cursor.execute(sql_query)
            conn.commit()
        elif len(row) == 2:
            sql_query = """
            exec ACTUALIZAR_CAMPO_MZS_CONDOMINIO '{ubigeo}', '{zona}'
                """.format(ubigeo=str(row[0]), zona=str(row[1]))
            cursor.execute(sql_query)
        conn.commit()
    conn.close()

def ActualizaIdPadreUrbano(ubigeos):
    conn = Conexion()
    cursor = conn.cursor()
    for ubigeo in ubigeos:
        sql_query="""
            exec USP_ACTUALIZA_ID_REG_OR_PADRE_URB '{ubigeo}'
            """.format(ubigeo=ubigeo)
        cursor.execute(sql_query)
    conn.commit()
    conn.close()



def Actualizar_MZS_AEU():
    server = "192.168.200.250"
    user = "sde"
    password = "$deDEs4Rr0lLo"

    conn = pymssql.connect(server, user, password, "CPV_SEGMENTACION")
    cursor = conn.cursor()
    cursor.execute("""
        exec ACTUALIZAR_MZS_AEU
        """)
    conn.commit()
    conn.close()



def LimpiarRegistrosSegmentacionTabularUbigeo(data):
    conn = Conexion()
    cursor = conn.cursor()

    for row in data:
        if len(row) == 1:
            sql_query = """
            exec LIMPIAR_REGISTROS_SEGM_TAB '{ubigeo}','{zona}'
            """.format(ubigeo=str(row[0]), zona="99999")
            cursor.execute(sql_query)
        elif len(row) == 2:
            sql_query = """
                exec LIMPIAR_REGISTROS_SEGM_TAB '{ubigeo}','{zona}'
                """.format(ubigeo=str(row[0]), zona=str(row[1]))
            cursor.execute(sql_query)

        conn.commit()
    conn.close()


def InsertarAdyacencia():
    conn = Conexion2()
    cursor = conn.cursor()
    cursor.execute("""
    exec SDE.INSERTAR_LISTA_ADYACENCIA
    """)
    conn.commit()
    conn.close()

def LimpiarRegistrosSegmentacionEspUbigeo(data):
    conn = Conexion()
    cursor = conn.cursor()

    for row in data:

        if len(row)==1:
            sql_query="""
            exec LIMPIAR_REGISTROS_SEGM_ESP '{ubigeo}','{zona}'
            """.format(ubigeo=str(row[0]),zona="99999")
            cursor.execute(sql_query)
        elif len(row)==2:
            sql_query = """
                exec LIMPIAR_REGISTROS_SEGM_ESP '{ubigeo}','{zona}'
                """.format(ubigeo=str(row[0]), zona=str(row[1]))
            cursor.execute(sql_query)

        conn.commit()
    conn.close()


def LimpiarRegistrosMatrizAdyacencia(data):
    conn = Conexion()
    cursor = conn.cursor()

    for row in data:
        if len(row)==1:
            sql_query="""
            exec LIMPIAR_REGISTROS_MATRIZ_ADYACENCIA '{ubigeo}','{zona}'
            """.format(ubigeo=str(row[0]),zona="99999")
            cursor.execute(sql_query)
        elif len(row)==2:
            sql_query = """
                exec LIMPIAR_REGISTROS_MATRIZ_ADYACENCIA '{ubigeo}','{zona}'
                """.format(ubigeo=str(row[0]), zona=str(row[1]))
            cursor.execute(sql_query)

        conn.commit()
    conn.close()



def ActualizarEstadoAEUSegmTab(data):
    conn = Conexion()
    cursor = conn.cursor()
    for row in data:
        if len(row) == 1:
            sql_query = """
            exec ACTUALIZAR_ESTADO_AEU_SEGM_TAB '{ubigeo}','{zona}'
            """.format(ubigeo=str(row[0]), zona="99999")
            cursor.execute(sql_query)
        elif len(row) == 2:
            sql_query = """
                exec ACTUALIZAR_ESTADO_AEU_SEGM_TAB '{ubigeo}','{zona}'
                """.format(ubigeo=str(row[0]), zona=str(row[1]))
            cursor.execute(sql_query)

        conn.commit()
    conn.close()


def ActualizarCantidadPaginas(fase,codigo,cant_pag,ruta,nom_web):
    conn=Conexion()
    cursor = conn.cursor()
    sql_query = """
            exec USP_ACTUALIZA_CANT_PAG '{}','{}', {},'{}' ,'{}'
            """.format(fase,codigo,cant_pag,ruta,nom_web)

    print sql_query
    cursor.execute(sql_query)

    conn.commit()
    conn.close()






def obtener_informacion_reporte_estudiantes(ubigeo, zona, subzona, fase='CPV2017'):
    conn = Conexion()
    cursor = conn.cursor()

    sql_query_cabecera = """
                select ccdd,departamento,ccpp,provincia,ccdi,distrito,codccpp,nomccpp,etiq_zona from MARCO_ZONA
                where UBIGEO='{}' AND ZONA='{}' and FASE='{}'""".format(ubigeo,zona,fase)
    cursor.execute(sql_query_cabecera)
    cabecera = []


    for row in cursor:
        cabecera=row
    print cabecera
    cabecera=list(cabecera)
    cabecera.append(subzona)

    if int(subzona)!=0:
        sql_where=' A.UBIGEO={ubigeo} and A.ZONA={zona} AND C.SUBZONA={subzona}'.format(ubigeo=ubigeo,zona=zona,subzona=subzona)
        #cabecera.append(subzona)
    else:
        sql_where = ' A.UBIGEO={ubigeo} and A.ZONA={zona} '.format(ubigeo=ubigeo,zona=zona)

    sql_query_data='''
    SELECT    ROW_NUMBER() OVER (ORDER BY A.UBIGEO, A.CODCCPP, A.ZONA, A.MANZANA, A.ID_REG_OR) viv_num
    ,C.SECCION seccion , A.MANZANA manzana,A.FRENTE_ORD  frente_ord ,D.P20_NOMBRE tipo_via,  A.P21 nombre_via,  A.P22_A n_puerta, A.P23 block, A.P24 mz
    ,A.P25 lote, A.P26 piso, A.P27_A interior , A.JEFE_HOGAR jefe_hogar, ISNULL(A.P33_1A_N,0) trab_sec_pub, ISNULL(A.P33_1B_N,0) est_uni,ISNULL(A.P33_1C_N,0) est_5_secund
    FROM    CPV_SEGMENTACION_GDB.sde.TB_CPV0301_VIVIENDA_U A
    INNER JOIN   CPV_SEGMENTACION_GDB.sde.TB_TIPO_VIA    D   ON D.P20=A.P20
    inner join CPV_SEGMENTACION.DBO.SEGM_U_VIV  B ON A.UBIGEO=B.UBIGEO COLLATE DATABASE_DEFAULT AND A.ZONA =B.ZONA COLLATE DATABASE_DEFAULT AND A.MANZANA=B.MANZANA COLLATE DATABASE_DEFAULT AND A.ID_REG_OR=B.ID_REG_OR   AND B.FASE='CPV2017'
	inner join CPV_SEGMENTACION.DBO.SEGM_U_AEU C ON B.UBIGEO=C.UBIGEO COLLATE DATABASE_DEFAULT AND B.ZONA=C.ZONA COLLATE DATABASE_DEFAULT AND B.AEU=C.AEU  COLLATE DATABASE_DEFAULT AND C.FASE='CPV2017'
    WHERE A.P29 IN (1,3) AND ((isnull(A.P33_1A_N,0)<>0 ) or ( isnull(A.P33_1B_N,0)<> 0 ) or ( isnull(A.P33_1C_N,0)<> 0 ) ) AND
    {sql_where}
    '''.format(sql_where=sql_where)
    print  sql_query_data


    cursor.execute(sql_query_data)
    data=[]

    for row in cursor:
        data.append(row)
    conn.commit()
    conn.close()
    return [cabecera,data]

def obtener_informacion_reporte_emp_especial(ubigeo,zona='',subzona='',tipo=1,fase='CPV2017'):
    conn = Conexion()
    cursor = conn.cursor()


    if zona!="":
        sql_query_cabecera = """
                    select a.ccdd,a.departamento,a.ccpp,a.provincia,a.ccdi,a.distrito,a.codccpp,a.nomccpp,a.etiq_zona,b.subzona,a.zona
                    from CPV_SEGMENTACION.DBO.SUBZONA b inner join
                    CPV_SEGMENTACION.DBO.marco_zona a on a.ubigeo=b.ubigeo and a.zona=b.zona and a.fase=b.fase
                    where a.UBIGEO='{}' AND a.ZONA='{}' and b.subzona='{}'  and a.FASE='{}'""".format(ubigeo, zona,subzona,fase)
        cursor.execute(sql_query_cabecera)
    else:

        sql_query_cabecera = """
                    select ccdd,nombdep departamento,ccpp,nombprov provincia,ccdi,nombdist distrito from MARCO_DISTRITO
                    where UBIGEO='{}' and FASE='{}' """.format(ubigeo, fase)
        cursor.execute(sql_query_cabecera)

    cabecera = []

    for row in cursor:
        cabecera = row
    print cabecera
    cabecera = list(cabecera)


    if zona != '':
        sql_where = " a.UBIGEO='{ubigeo}' and a.ZONA='{zona}' and b.SUBZONA='{subzona}' ".format(ubigeo=ubigeo, zona=zona,subzona=subzona)
    else:
        sql_where = " a.UBIGEO='{ubigeo}' ".format(ubigeo=ubigeo, zona=zona)


    #sql_query_data = '''
#
    #    SELECT  ROW_NUMBER() OVER (ORDER BY A.UBIGEO, A.CODCCPP, A.ZONA, A.MANZANA, A.ID_REG_OR) viv_num
    #    ,D.ETIQ_ZONA zona , rtrim(A.MANZANA) manzana,A.FRENTE_ORD  frente_ord ,B.P20_NOMBRE tipo_via,  A.P21 nombre_via,  A.P22_A n_puerta, A.P23 block, A.P24 mz
    #    ,A.P25 lote, A.P26 piso, A.P27_A interior , A.P28 km,'' nom_est , ISNULL(A.P35,'') razon_soc ,CASE WHEN ISNULL(P38,0) IN (999,99999) THEN 0 ELSE ISNULL(P38,0) END cant_pob
    #    ,A.ID_REG_OR
    #    FROM    CPV_SEGMENTACION_GDB.sde.TB_CPV0301_VIVIENDA_U A
    #    INNER JOIN CPV_SEGMENTACION_GDB.sde.TB_TIPO_VIA    B   ON B.P20=A.P20
    #    INNER JOIN  (SELECT DISTINCT CODIGOCIU,TIPO FROM  CPV_SEGMENTACION_GDB.sde.MARCO_CODIGOS_ESTABLECIMIENTOS) C ON A.P36=C.CODIGOCIU
#
    #    INNER JOiN ( SELECT DISTINCT UBIGEO,ZONA,ETIQ_ZONA FROM CPV_SEGMENTACION.DBO.MARCO_ZONA) D ON D.UBIGEO=A.UBIGEO COLLATE DATABASE_DEFAULT AND D.ZONA=A.ZONA COLLATE DATABASE_DEFAULT
    #    INNER JOIN CPV_SEGMENTACION.dbo.SEGM_U_VIV  F ON A.UBIGEO =F.UBIGEO COLLATE DATABASE_DEFAULT AND A.ZONA=F.ZONA COLLATE DATABASE_DEFAULT AND A.MANZANA=F.MANZANA COLLATE DATABASE_DEFAULT AND A.ID_REG_OR=F.ID_REG_OR
    #    INNER JOIN (SELECT * FROM CPV_SEGMENTACION.DBO.SEGM_U_AEU WHERE FASE='CPV2017') E ON F.UBIGEO=E.UBIGEO COLLATE DATABASE_DEFAULT AND F.ZONA=E.ZONA COLLATE DATABASE_DEFAULT AND F.AEU=E.AEU COLLATE DATABASE_DEFAULT
    #    WHERE A.P29 IN (2,3,4)
    #    AND
    #    {sql_where} and C.TIPO={tipo}
    #    '''.format(sql_where=sql_where,tipo=tipo)






    #sql_query_data=" select ROW_NUMBER() OVER (ORDER BY UBIGEO,ZONA, MANZANA ) viv_num, etiq_zona,manzana,frente_ord,tipo_via,nombre_via,n_puerta,block,mz," \
    #               "lote,piso,interior,km,nom_est,razon_soc,cant_pob,'0' id_reg_or  from REPORTE_EMPADRONIENTO_ESPECIAL  where {sql_where} and ID_TIPO_EST={tipo} AND AREA='Uubano' AND ZONA NOT IN (0,'') ".format(sql_where=sql_where,tipo=tipo)

    sql_query_data='''
    select ROW_NUMBER() OVER (ORDER BY a.UBIGEO,a.ZONA, a.MANZANA ) viv_num, a.zona,a.manzana,
    a.frente_ord,a.tipo_via,a.nombre_via,a.n_puerta,a.block,a.mz,a.lote,a.piso,a.interior,
    a.km,a.nom_est,''razon_soc,cant_per,'0' id_reg_or 
    
    from dbo.DIRECTORIO_EMP_ESPECIAL a
    inner join  (
	SELECT UBIGEO,ZONA,MANZANA,min(SUBZONA) SUBZONA FROM 
	dbo.SEGM_U_RUTA
	GROUP BY UBIGEO,ZONA,MANZANA)   b on a.ubigeo=b.ubigeo and a.zona=b.zona and a.manzana=b.manzana
    where  {sql_where} and a.ID_TIPO_EST={tipo} AND a.AREA='Urbano'  and isnull(a.manzana,'')<>''  '''.format(sql_where=sql_where,tipo=tipo)


    print  sql_query_data

    cursor.execute(sql_query_data)
    data = []

    print 'cantidad de registros',len(data)
    for row in cursor:
        data.append(row)
    conn.commit()
    conn.close()
    return [cabecera, data]










def actualizar_errores_input_adicionales(ubigeo, zona):
    conn=Conexion2()
    cursor = conn.cursor()
    sql_query = """
            exec USP_ERRORES_INPUT_ADICIONALES '{}', '{}'
            """.format(ubigeo, zona)
    cursor.execute(sql_query)
    conn.commit()
    conn.close()



def listarResultadoDirectorioFenomeno(codigo):

    conn=Conexion()
    cursor = conn.cursor()


    if len(codigo)==6:
        sql_query = """
            SELECT * FROM
            FENOMENO_MARCO_VIVIENDA
            where ubigeo='{}'
            order by ubigeo,codccpp,zona,manzana,id_reg_or
            """.format(codigo)



    else:
        sql_query = """
            SELECT * FROM
            FENOMENO_MARCO_VIVIENDA
            where SUBSTRING(UBIGEO,1,2)='{}'
            order by ubigeo,codccpp,zona,manzana,id_reg_or
            """.format(codigo)

    cursor.execute(sql_query)
    columns = [column[0] for column in cursor.description]
    registros = []
    for row in cursor.fetchall():
        registros.append(row)

    conn.close()

    return [columns,registros]



#
def listarDistritosDirectorioFenomeno():

    conn=Conexion()
    cursor = conn.cursor()
    sql_query = """
    SELECT * FROM
    FENOMENO_MARCO_DISTRITO
    WHERE UBIGEO IN (
'021809',
'040104',
'040112',
'060101',
'110101',
'130102',
'140101',
'140105',
'200601',
'200701',
'200702',
'200801'

)

    order by 1
    """.format()
    cursor.execute(sql_query)

    registros = []
    for row in cursor.fetchall():
        registros.append(row)

    return registros


def listarDepDirectorioFenomeno():

    conn=Conexion()
    cursor = conn.cursor()
    sql_query = """
    SELECT * FROM
    DEP_RESULTADO_DIRECTORIO_FENOMENO
    order by 1
    """.format()
    cursor.execute(sql_query)

    registros = []
    for row in cursor.fetchall():
        registros.append(row)

    return registros


def actualizarExportDistrito(ubigeo):

    conn=Conexion()
    cursor = conn.cursor()
    sql_query = """
    UPDATE FENOMENO_MARCO_DISTRITO
    SET EXPORTADO=1
    WHERE UBIGEO='{}'
    """.format(ubigeo)
    cursor.execute(sql_query)
    conn.commit()
    conn.close()

def actualizarExistenciaEjesViales(ubigeo,zona,flag):
    conn=Conexion()
    cursor = conn.cursor()


    sql_query = """
    UPDATE TB_ZONA
    SET TIENE_EJES_VIALES=1
    WHERE UBIGEO='{ubigeo}' AND ZONA='{zona}'

    UPDATE TB_ZONA
    SET FLAG_CALIDAD=1
    WHERE UBIGEO='{ubigeo}' AND ZONA='{zona}'


        """.format(ubigeo=ubigeo,zona=zona,flag=flag)


    cursor.execute(sql_query)
    conn.commit()
    conn.close()

def resetear_zonas_calidad():
    connx =Conexion()
    cursor = connx.cursor()
    sql_query = """
        begin
        update  a
        set a.flag_proc_calidad_input=0
        from tb_zona a
        end
        """
    cursor.execute(sql_query)

    connx.commit()
    connx.close()


def listar_ccpp_rural():
    connx=Conexion()

    cursor = connx.cursor()
    sql_query = """
    begin
    select ubigeo,idruta,codccpp,cant_dias_emp,dia_ini_emp,or_ccpp from dbo.segm_r_ccppruta a
    where IDRUTA='0306020000000104'
    order by idruta,or_ccpp;

    end

    """
    cursor.execute(sql_query)

    registros = []
    for row in cursor.fetchall():
        registros.append(row)
    connx.close()
    return registros


def obtener_flag_segm_u_distrito(ubigeo,fase):

    connx=Conexion()

    cursor = connx.cursor()
    sql_query = """
    begin
    select flag_segm_u from marco_distrito a
    where ubigeo='{}' and fase='{}'
    end
    """.format(ubigeo,fase)
    cursor.execute(sql_query)

    estado=0
    for row in cursor.fetchall():
        estado=row[0]
    connx.close()
    return estado


#print ObtenerReporteDistrital('15060400','III-EXPERIMENTAL')



def actualizar_existencia_capas_por_zona():

    connx =Conexion()
    cursor = connx.cursor()
    sql_query = """
        begin
        EXEC dbo.USP_ACTUALIZAR_EXIST_CAPAS_POR_ZONA
        end
        """
    cursor.execute(sql_query)

    connx.commit()
    connx.close()



def actualizar_monitoreo_segmentacion(ubigeo,fase):
    connx=Conexion3()
    cursor = connx.cursor()
    sql_query = """
        begin
        EXEC sde.USP_ACTUALIZAR_MONITOREO_SEGMENTACION '{fase}','{ubigeo}'
        end
        """.format(fase=fase,ubigeo=ubigeo)
    cursor.execute(sql_query)

    connx.commit()
    connx.close()



def actualizar_viviendas_calidad(ubigeo,zona,aeu='',fase='CPV2017'):
    connx = Conexion()
    cursor = connx.cursor()
    sql_query = """
            begin
            EXEC dbo.USP_ACTUALIZAR_VIVIENDAS_CALIDAD '{ubigeo}','{zona}','{aeu}'
            end
            """.format( ubigeo=ubigeo,zona=zona,aeu=aeu)
    cursor.execute(sql_query)

    connx.commit()
    connx.close()


def obtener_lista_zonas_inserccion():
    conn = Conexion()
    cursor = conn.cursor()
    sql_query = """
                select top 1 ubigeo,zona from marco_zona
                where flag_data_insert=0 and flag_proc_segm=1 and fase='CPV2017' and substring(ubigeo,1,2) in ('14','20')

                """.format()
    print sql_query
    cursor.execute(sql_query)
    data=[]

    for row in cursor:
        data.append(["{}".format(row[0]),"{}".format(row[1])])
    conn.commit()
    conn.close()
    return data




def obtener_lista_zonas_reproceso():
    conn = Conexion()
    cursor = conn.cursor()
    sql_query = """
            declare @table TABLE(
                    ubigeo varchar(6),
                    zona varchar(6)
                    );

            insert @table


            select top 1 a.ubigeo,a.zona from marco_zona a
            where
            a.fase='CPV2017'  and flag_reproc=1 and flag_proc_segm=1
            order by a.ubigeo ,a.zona

            update  a
             set a.flag_reproc=3,a.n_reproc=a.n_reproc+1
             from marco_zona a
             inner join @table b on a.ubigeo=b.ubigeo and a.zona=b.zona and a.fase='CPV2017'

             select ubigeo,zona from @table
                --select top 1 ubigeo,zona from marco_zona
                ---where flag_reproc=1 and flag_proc_segm=1 and fase='CPV2017'
            


                """.format()
    print sql_query
    cursor.execute(sql_query)
    data=[]

    for row in cursor:
        data.append(["{}".format(row[0]),"{}".format(row[1])])
    conn.commit()
    conn.close()
    return data






def actualizar_flag_reproceso_segm(ubigeo,zona,fase='CPV2017'):
    conn = Conexion()
    cursor = conn.cursor()

    sql_query = """
    begin

    update a
    set a.flag_reproc='0' ,a.n_reproc=a.n_reproc+1
    from marco_zona a
    where a.ubigeo='{ubigeo}' and a.zona='{zona}' and a.fase='{fase}'
    end
    """.format(ubigeo=ubigeo,zona=zona,fase=fase)
    cursor.execute(sql_query)

    conn.commit()
    conn.close()

