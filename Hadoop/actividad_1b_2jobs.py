#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Sep 27 17:38:36 2021

@author: marcelo
"""

"""
1) b) La página más visitada (en cuanto a tiempo de visita) por cada usuario siempre que la
página haya sido visitada más de V veces por ese usuario (V es parámetro de la consulta).
"""

from MRE import Job

root_path = 'dataset/'

inputDir = root_path + "/input/"
tmpOutputDirJ1 = root_path + "/b_output/tmp_output_j1/"
outputDir = root_path + "/b_output/output/"

'''
Recibe IDCliente, IDPagina, Tiempo
Retorna (IDCliente, IDPagina), Tiempo
'''
def fmap1a(key, value, context):
    v = value.split('\t')
    context.write((key,v[0]), v[1])

'''
Se recibe como clave (IDCliente, IDPagina) y se acumulan todos los tiempos de permanencia para esa clave.
'''    
def fred1a(key, values, context):
    params = context["max_visitas"]
    acum=0
    cant=0
    for v in values:
        acum=acum+int(v)
        cant=cant+1
    
    if(cant >= params[0]):
        context.write(key, acum)

'''
Recibe IDCliente, IDPagina, Tiempo Total
Retorna IDCliente, (IDPagina, Tiempo Total)
'''
def fmap1b(key, value, context):
    v = value.split('\t')
    context.write(key, (v[0], v[1]))

'''
Recibe como clave IDCliente y como values una lista de tuplas (IDPagina, Tiempo total). Halla el máximo tiempo
para cada página.
Retorna IDCliente, (IDPagina, Máximo tiempo)
'''    
def fred1b(key, values, context):
    maxi=-1
    pag=-1
    for v in values:
        if int(v[1]) > int(maxi):
            maxi=v[1]
            pag=v[0]
    context.write(key, (pag, maxi))


job = Job(inputDir, tmpOutputDirJ1, fmap1a, fred1a)
params = {"max_visitas": [2]}
job.setParams(params)

if job.waitForCompletion():
    job2 = Job(tmpOutputDirJ1, outputDir, fmap1b, fred1b)
    job2.waitForCompletion()    
