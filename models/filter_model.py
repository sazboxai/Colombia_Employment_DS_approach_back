from sqlalchemy import create_engine
import pandas as pd
import os
from lifelines import KaplanMeierFitter

def engine_creator():
    bd_name = os.getenv('DB_NAME')
    bd_pass = os.getenv('DB_PASS')
    bd_user = os.getenv('DB_USER')
    bd_host = os.getenv('DB_HOST')
    bd_url = 'postgres://' + bd_user + ':' + bd_pass + '@' + bd_host + '/' + bd_name
    engine = create_engine(bd_url)
    return engine




def table_query(query, engine=engine_creator()):
    df = pd.read_sql_query(query, con=engine)
    return (df)




def table_builder(table, array_filters= []):
    base= "select * from " + str(table)
    if len(array_filters) > 0:
        base = base + " where " + array_filters.pop(0)
        if len(array_filters) >0:
            for el in array_filters:
                base = base + " and " + el
    print(base)
    print("++++++++++++++++++++++++++++++++++++")
    tabla = table_query(base)
    return tabla

def agg_builder_percent(table= "personas", agg_val="p6020", agg_val2= "dpto"):

    base= """with temp as (SELECT dpto, p6020, count(*) AS percentage 
            FROM   personas 
            group by 1,2)
            
            select t.dpto, t.p6020, float4(t.percentage)/cuenta as percentage
            from temp t
            left join (select dpto, count(*) as cuenta from personas group by 1) t2
            on t.dpto =t2.dpto
            """.replace("personas", table).replace("p6020", agg_val).replace("dpto", agg_val2)
    tabla = table_query(base)
    return tabla


def agg_builder_count(table="personas", agg_val="p6020", agg_val2="dpto", filter = "1=1"):
    base = """SELECT dpto, p6020, count(*) AS total 
            FROM   personas where filter_a
            group by 1,2
            """.replace("personas", table).replace("p6020", agg_val).replace("dpto", agg_val2).replace("filter_a", filter)
    tabla = table_query(base)
    return tabla

def group_rows(table="personas", agg_val="p6020", agg_val2="dpto", filter = "1=1"):
    base = """SELECT dpto, p6020
            FROM   personas where filter_a
            group by 1,2
            """.replace("personas", table).replace("p6020", agg_val).replace("dpto", agg_val2).replace("filter_a", filter)
    tabla = table_query(base)
    return tabla

def total_expansion(table="area_personas", agg_val="p6020", agg_val2="p6040", filter = "1=1"):
    base = """SELECT dpto, p6020,
                round(sum(fex_c_2011)) as total
            FROM   personas where filter_a
            group by 1,2
            """.replace("personas", table).replace("p6020", agg_val).replace("dpto", agg_val2).replace("filter_a", filter)
    tabla = table_query(base)
    return tabla


#departments, crear el filter builder

# Test de fillter builder so it can be iterated or used the pop pytho

def filter_builder(dict_values ):
    filters = []
    for el in dict_values:
        filters.append(" " + str(el)+ " in " + str(dict_values[el]).replace("[", "(").replace("]",")")+ " " )
    return filters



def ocupancy_rate(gender=None, month=1, municipality=None, age_base=None, age_top=None, estado_civil= None,  agregador = None):


    query = ("""with temp as (
    select
    per.AREA as municipio,
    p2.valor as sexo,
    per.p6040 as edad,
    per.MES as mes,
    p1.valor as nivel_educ,
    per.esc as escolaridad,
    p3.valor as estado_civil,
    coalesce(ocu.p760, des.p7250/4) as tiempo_buscando,
    coalesce(ocu.fex_c_2011, des.fex_c_2011) as factor,
           case when ocu.es_ocupado then 1
             when des.es_desocupado then 0
               else null end ocupado
    from area_personas as per
    left join area_ocupados as ocu
    on
    per.directorio=ocu.directorio and
    per.secuencia_p=ocu.secuencia_p and
    per.orden=ocu.orden and
    per.hogar=ocu.hogar
    left join  area_desocupados as des
    on
    per.directorio=des.directorio and
    per.secuencia_p=des.secuencia_p and
    per.orden=des.orden and
    per.hogar=des.hogar
    left join p6220 p1 on p1.llave = per.p6220
    left join p6020 p2 on p2.llave = per.p6020
    left join p6070 p3 on p3.llave = per.p6070)

    select * from temp 
    where ocupado is not null and mes = M_change""".replace("M_change", str(month)))



    if gender is not None:
        query = query + " and sexo = " + "'"+ gender + "' "
    if municipality is not None :
        query = query + " and municipio = " + str(municipality) + " "
    if age_base is not None:
        query = query + " and edad >= " + str(age_base) + " "
        #base = base[base["edad"] >= age_base]
    if age_top is not None:
        #base = base[base["edad"] <= age_top]
        query = query + " and edad <= " + str(age_top) + " "
    if estado_civil is not None:
        query = query + " and estado_civil = " +"'"+ str(estado_civil) + "' "
        #base = base[base["estado_civil"] == estado_civil]

    base = table_query(query)
    base['factor'] = base['factor'].round()
    base = base.loc[base.index.repeat(base.factor)]

    if agregador is not None:
        resp = base.groupby([ agregador, "ocupado"]).agg({'factor': 'count'})
        resp["tasa"] = resp.groupby(level=0).apply(lambda x: 100 * x / float(x.sum()))
        resp = resp.reset_index()
    else:
        resp = base.groupby([ "ocupado"]).agg({'factor': 'count'})
        resp["tasa"] = (resp["factor"] / (resp["factor"].sum()))*100
        resp = resp.reset_index()

    resp = resp.rename(columns= {"factor":"total"})



    return resp


def curvas(factor, df_dic, percentil=0.25):
    kmf = KaplanMeierFitter()
    df_null = None
    for el in df_dic[factor].unique():
        print(el)
        if el is None:
            pass
        else:
            kmfn = kmf.fit(df_dic[df_dic[factor] == el]["tiempo_buscando"], df_dic[df_dic[factor] == el]["ocupado"],
                           label='pct')
            temp = kmfn.survival_function_
            temp = temp.reset_index()
            temp["fact"] = el
            try:
                temp["percentil"] = kmfn.percentile(percentil)
            except:
                temp["percentil"] = 0
            df_null = temp.append(df_null)

    df_null = df_null.reset_index()
    return df_null


def survival_curves(gender=None, month=1, municipality=None, age_base=None, age_top=None, estado_civil=None,
                    agregador=None, percentil=0.25):
    query = ("""with temp as (
    select
    per.AREA as municipio,
    p2.valor as sexo,
    per.p6040 as edad,
    per.MES as mes,
    p1.valor as nivel_educ,
    per.esc as escolaridad,
    p3.valor as estado_civil,
    coalesce(ocu.p760, des.p7250/4) as tiempo_buscando,
    coalesce(ocu.fex_c_2011, des.fex_c_2011) as factor,
           case when ocu.es_ocupado then 1
             when des.es_desocupado then 0
               else null end ocupado
    from area_personas as per
    left join area_ocupados as ocu
    on
    per.directorio=ocu.directorio and
    per.secuencia_p=ocu.secuencia_p and
    per.orden=ocu.orden and
    per.hogar=ocu.hogar
    left join  area_desocupados as des
    on
    per.directorio=des.directorio and
    per.secuencia_p=des.secuencia_p and
    per.orden=des.orden and
    per.hogar=des.hogar
    left join p6220 p1 on p1.llave = per.p6220
    left join p6020 p2 on p2.llave = per.p6020
    left join p6070 p3 on p3.llave = per.p6070)

    select * from temp 
    where ocupado is not null and mes = M_change""".replace("M_change", str(month)))

    if gender is not None:
        query = query + " and sexo = " + "'" + gender + "' "
    if municipality is not None:
        query = query + " and municipio = " + str(municipality) + " "
    if age_base is not None:
        query = query + " and edad >= " + str(age_base) + " "
        # base = base[base["edad"] >= age_base]
    if age_top is not None:
        # base = base[base["edad"] <= age_top]
        query = query + " and edad <= " + str(age_top) + " "
    if estado_civil is not None:
        query = query + " and estado_civil = " + "'" + str(estado_civil) + "' "
        # base = base[base["estado_civil"] == estado_civil]

    base = table_query(query)
    base['factor'] = base['factor'].round()
    base = base.loc[base.index.repeat(base.factor)]
    base = base.reset_index()
    base = base[~pd.isnull(base["tiempo_buscando"])]

    if agregador is not None:
        resp = curvas(agregador, base, percentil)
    else:
        kmf = KaplanMeierFitter()
        kmfn = kmf.fit(base["tiempo_buscando"], base["ocupado"], label='pct')
        temp = kmfn.survival_function_
        temp = temp.reset_index()
        temp["percentil"] = kmfn.percentile(percentil)
        resp = temp

    return resp