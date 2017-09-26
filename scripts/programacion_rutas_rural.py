import conection_sql as conx


lista_rutas_ccpp_rural=conx.listar_ccpp_rural()

#print lista_rutas_ccpp_rural


lista_rutas_ccpp_rural_dias=[]

fila=[]

for el in lista_rutas_ccpp_rural:
    ubigeo=el[0]
    idruta=el[1]
    codccp=el[2]
    cant_dias=int(el[3])
    dia_ini=int(el[4])
    or_ccpp = int(el[5])


    for i in range(cant_dias):
        dia=i+dia_ini
        lista_rutas_ccpp_rural_dias.append([ubigeo,idruta,codccp,dia,or_ccpp])

lista_rutas_ccpp_rural_dias_temp=lista_rutas_ccpp_rural_dias[:]


for el in lista_rutas_ccpp_rural_dias:

    print el

lista_final=[]

while len(lista_rutas_ccpp_rural_dias_temp)>0:
    fila = []
    #encontro=0
    for i in range(15):
        encontro = 0
        if len(lista_rutas_ccpp_rural_dias_temp)>0:

            for ruta_ccpp in lista_rutas_ccpp_rural_dias_temp:
                if ruta_ccpp[3]==(i+1):
                    fila.append(lista_rutas_ccpp_rural_dias_temp[0][2])
                    lista_rutas_ccpp_rural_dias_temp.remove(ruta_ccpp)
                    encontro=1
                    break

            if encontro==0:
                fila.append('')

                #dia_temp=lista_rutas_ccpp_rural_dias_temp[0][3]


            #if (i+1)==dia_temp:
            #    fila.append(lista_rutas_ccpp_rural_dias_temp[0][2])
            #    del lista_rutas_ccpp_rural_dias_temp[0]
            #else:
            #    fila.append('')
            #print lista_rutas_ccpp_rural_dias_temp

        else:
            fila.append('')
    #print fila
    lista_final.append(fila)

for el in lista_final:
    print el