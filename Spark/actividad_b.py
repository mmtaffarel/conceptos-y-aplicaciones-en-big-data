#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Nov 18 15:12:19 2021

@author: Marcelo Taffarel
"""
"""
Actividad b.

Dado los DNI de dos individuos i 1 y i 2 determinar si i 1 es ancestro de i 2.
"""

"""
Se hace un join entre un descendiente (DNI2 al inicio) y la RDD de personas para determinar su ancestro.
Se itera mientras el ancestro no sea igual a DNI1 y en cada iteración se actualizan descendiente y ancestro.
Si en algún momento un ancestro es igual a DNI1 se imprime y sale. Si nunca un ancestro es igual a DNI1 se
finaliza cuando no existen más ancestros hacia arriba en el árbol.
"""

import os

#os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"     
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-9-oracle"

from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("test").setMaster("local")

sc = SparkContext.getOrCreate(conf=conf)

dataset = "dataset/"
inputDir = "input/"
outputDir = "a_output/"
fileName = "Genealogia.txt"
#fileName = "test.txt"
fileInputPath = dataset + inputDir + fileName;
dirOutputFile = dataset + outputDir

'''
Ingreso de los DNI por teclado
'''
dni1 = input("Ingrese el primer DNI: ")
dni2 = input("Ingrese el segundo DNI: ")

personas = sc.textFile(fileInputPath)
bc1 = sc.broadcast([dni1])
bc2 = sc.broadcast([dni2])

def ffilter1(t):
    if(t[1] in bc1.value):
        return t

def ffilter2(t):
    if(t[1] in bc2.value):
        return t

personas = personas.map(lambda line: line.split("\t"))
dni1 = personas.filter(ffilter1)
dni2 = personas.filter(ffilter2)

resDni1 = dni1.collect();
resDni2 = dni2.collect();

personas = personas.map(lambda t: (t[1], (t[0], t[2])))
descendiente = dni2.map(lambda t: (t[2], (t[0], t[1])))
ancestro = descendiente.join(personas)
    
while True:
    
    resAncestro = ancestro.collect()
    if(not resAncestro):
        print ("El DNI: " + resDni1[0][1] + " no es ancestro del DNI: " + resDni2[0][1])
        break

    if((resAncestro[0][0] == resDni1[0][1])):
        print ("El DNI: " + resAncestro[0][0] + " es ancestro del DNI: " + resDni2[0][1])
        break
    else:
        descendiente = sc.parallelize([[resAncestro[0][1][1][0],resAncestro[0][0],resAncestro[0][1][1][1]]])
        dniDescendienteTmp = descendiente.map(lambda t: (t[2], (t[0], t[1])))
        ancestro = dniDescendienteTmp.join(personas)
        

