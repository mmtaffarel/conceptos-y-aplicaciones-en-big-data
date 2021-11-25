#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Sep 27 17:38:36 2021

@author: marcelo

ACLARACIÓN: cada cliente único se cuenta como una visita. Es decir, si un cliente visitó muchas veces la misma
página, cuenta solo como una visita.-
"""

"""
1) c) La página más visitada (en cuanto a cantidad de visitas, sin importar el tiempo de permanencia) por al 
menos U usuarios distintos (U es parámetro de la consulta).
"""

from MRE import Job

root_path = 'dataset/'

inputDir = root_path + "/input/"
tmpOutputDirJ1 = root_path + "/c_output/tmp_output_j1/"
tmpOutputDirJ2 = root_path + "/c_output/tmp_output_j2/"
outputDir = root_path + "/c_output/output/"

'''
Recibe IDCliente, IDPagina, Tiempo
Retorna (IDCliente, IDPagina), 1
'''
def fmap1a(key, value, context):
    v = value.split('\t')
    context.write((v[0], key), 1)

'''
Como se recibe como clave (IDCliente, IDPagina) y solo se escribe una vez (se eliminan duplicados), 
se retorna cada pagina diferente que visitó un usuario
'''  
def fred1a(key, values, context):
    context.write(key, 1)
    
'''
Recibe IDPagina, IDCliente, 1
Retorna IDPagina, IDCliente
'''
def fmap1b(key, value, context):
    v = value.split('\t')
    context.write(key, v[0])

'''
Recibe como clave IDPagina y como values una lista de IDs de clientes diferentes. Acumula y retorna
(IDPagina, CantClientes) sólo para aquellos casos en que cantidad de clientes es mayor al parámetro 
definido
'''  
def fred1b(key, values, context):
    params = context["max_clientes"]
    c=0
    for v in values:
        c=c+1
    
    if(c >= int(params[0])):
        context.write(key, c)

'''
Recibe IDPagina, Cantidad Clientes
Retorna 1, (IDPagina, Cantidad Clientes). Envía todo al mismo reducer para calcular la página más visitada.
'''
def fmap1c(key, value, context):
    context.write(1, (key, value))

'''
Recibe como clave 1 y como values una lista de tuplas (IDPagina, Cantidad Clientes). Halla la página
con más visitas
Retorna IDPagina, Cantidad Clientes
'''    
def fred1c(key, values, context):
    maxi=-1
    pag=-1
    for v in values:
        if int(v[1]) > int(maxi):
            maxi=v[1]
            pag=v[0]
    context.write(pag, maxi)

job = Job(inputDir, tmpOutputDirJ1, fmap1a, fred1a)

if job.waitForCompletion():
    job2 = Job(tmpOutputDirJ1, tmpOutputDirJ2, fmap1b, fred1b)
    params = {"max_clientes": [2]}
    job2.setParams(params)
    job2.waitForCompletion()
    
    if job2.waitForCompletion():
        job3 = Job(tmpOutputDirJ2, outputDir, fmap1c, fred1c)
        job3.waitForCompletion()    
