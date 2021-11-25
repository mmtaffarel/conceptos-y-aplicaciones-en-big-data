#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Sep 27 17:38:36 2021

@author: marcelo
"""

"""
1) a) El usuario que más páginas distintas visitó.
"""

from MRE import Job

root_path = 'dataset/'

inputDir = root_path + "/input/"
tmpOutputDirJ1 = root_path + "/a_output/tmp_output_j1/"
tmpOutputDirJ2 = root_path + "/a_output/tmp_output_j2/"
outputDir = root_path + "/a_output/output/"

'''
Recibe IDCliente, IDPagina, Tiempo
Retorna (IDCliente, IDPagina), 1
'''
def fmap1a(key, value, context):
    v = value.split('\t')
    context.write((key,v[0]), 1)

'''
Como se recibe como clave (IDCliente, IDPagina) y solo se escribe una vez (se eliminan duplicados), 
se retorna cada pagina diferente que visitó un usuario
'''  
def fred1a(key, values, context):
    context.write(key, 1)

'''
Recibe IDCliente, IDPagina, 1
Retorna (IDCliente), IDPagina
'''
def fmap1b(key, value, context):
    v = value.split('\t')
    context.write(key, v[0])

'''
Recibe como clave IDCliente y como values una lista de IDs de páginas diferentes. Acumula y retorna
(IDCliente, CantPaginas)
'''    
def fred1b(key, values, context):
    c=0
    for v in values:
        c=c+1
    context.write(key, c)

'''
Recibe IDCliente, CantPaginas. Retorna 1 como clave (para enviar todas las tuplas al mismo reducer) y una lista
de tuplas (IDCliente, CantPaginas)
'''
def fmap1c(key, value, context):
    context.write(1, (key, value))

'''
Calcula el cliente con la máxima cantidad de paginas diferentes visitadas y retorna una tupla (IDCliente, CantPaginas)
'''
def fred1c(key, values, context):
    maxi=0
    cli=0
    for v in values:
        if int(v[1]) > int(maxi):
            maxi=v[1]
            cli=v[0]
    
    context.write(cli, maxi)

job = Job(inputDir, tmpOutputDirJ1, fmap1a, fred1a)


if job.waitForCompletion():
    job2 = Job(tmpOutputDirJ1, tmpOutputDirJ2, fmap1b, fred1b)
    
    if job2.waitForCompletion():
        job3 = Job(tmpOutputDirJ2, outputDir, fmap1c, fred1c)
        job3.waitForCompletion()
    