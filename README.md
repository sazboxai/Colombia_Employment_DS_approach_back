# Server end points 

- url = `18.221.120.194:8020`
- `/factors/<table>` Return a json with the factors and their values. The tables that can be `sex_dict` , `edad_dict` , `education_dict` and `marital_dict`
- how to run it :
    - dont forget tu make build the .env file with structure like this 
        - `DB_NAME=postgres ...`
    - `sudo docker build -t ds4a .`
    - `sudo docker run -p 8020:8020 -d --restart=always --env-file test.env --name ds4a ds4a`
- filter builder is capable of filtering the flowing fields: 
    - `u_dpto`, `u_mpio`, `p_sexo`, `p_edad`, `p_est_civil`, `p_nivel_anos`
    - One example of the request at `/filtered_table`:
        - {
	"table":"per",
	"filters":{"u_dpto":["54"], "u_mpio":["405"],"p_sexo":["2.0"], "p_edad":["3.0"], "p_est_civil":["7.0"], "p_nivel_anos":["1.0"]}
}

- Agregaciones por factores se puedne consultar en el end point `ec2-3-133-150-215.us-east-2.compute.amazonaws.com:8020/agg_pct` la firma es del tipo:
    - {
	"tabla":"personas",
	"var_agg":"p6030S1",
	"agregador":"dpto"
      }
       - En este caso la tabla que se quiere analizar es personas la variable que se quiere agregar es el porcentaje de la edad  `var_agg` y el agregador sobre el cual se va a sacar el porcentaje es el `dpto`
       
     - Para la distribucion de genero el ejemplo seria {
	"tabla":"personas",
	"var_agg":"p6020",
	"agregador":"dpto"
        }
        
     - Pueden salir NAN y son casos en los que no se especifica la edad o el factor de agregacion 
 
 - `/build_count` is the same as the aggregator percentage but instead it counts 
  - `/groups_raw` brings both groups wihtout aggregating and you can add a filter like this :
  
    - - {
	"tabla":"area_personas",
	"var_agg":"p6020",
	"agregador":"p6040",
	"filtro":"mes=3"
      }
      
 - `/factor_x` It give you the whole population by aggregating within this two grups and adding by its expansion factor ej:
    - {
	"tabla":"area_personas",
	"var_agg":"p6020",
	"agregador":"p6040",
	"filtro":"mes=3"
      }

- `employement_rate` this end point allow you to analyze unemployment rate by several factors and filter yo can  remove any filter and it will work. The Only mandatory filter is month Ex:
    - {
"month":1,
"gender":"Hombre",
"municipality":11,
"age_base": 23,
"age_top":35,
"marital_status":"Esta soltero (a)",
"aggregator":"nivel_educ"}
    - {
"month":1,
"gender":"Hombre",
"municipality":11,
"age_base": 23,
"age_top":35,
"marital_status":"Esta soltero (a)"}
    - {"month":12,
"gender":"Mujer",
"municipality":11,
"age_base": 18,
"age_top":18,
"aggregator":"nivel_educ"}

- `survival` this brings you the survayval information with the same filter struction as the employment end point:

    - {
"month":1,
"gender":"Hombre",
"municipio":76,
"age_base": 23,
"age_top":35,
"marital_status":"Esta soltero (a)",
"aggregator":"nivel_educ"
	
}