#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Nov 18 15:12:19 2021

@author: Marcelo Taffarel
"""
"""
Actividad c.

El nombre de la "abuela" que tiene más descendientes
"""
"""
La idea es obtener una RDD solo con las super abuelas y desde ahí una lista para ir iterando por cada una de
ellas e ir contando sus descendientes. En cada ciclo del bucle for se arma una RDD con el abuelaID (DNI) y se
setea una variable broadcast con una lista que acumulará la cantidad de descendientes para esa abuela. Luego, el
bucle while va bajando de a un nivel por el árbol y en cada nivel actualiza la cantidad de ancestros de la abuela
actual, actualiza ancestros con descendientes y sigue iterando hasta que no hay mas descendientes. A la salida del
while se imprime la cantidad de descendientes para la abuela actual y se reinicia el ciclo con la siguiente.
"""

import os

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"     

from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("test").setMaster("local")

sc = SparkContext.getOrCreate(conf=conf)

dataset = "dataset/"
inputDir = "input/"
outputDir = "a_output/"
fileName = "Genealogia.txt"
fileInputPath = dataset + inputDir + fileName;
dirOutputFile = dataset + outputDir

def ffilter1(t):
    if t[2] == 'None':
        return t

def ffilter2(t):
    if t[2] != 'None':
        return t
    
personas = sc.textFile(fileInputPath)

personas = personas.map(lambda line: line.split("\t"))
abuelas = personas.filter(ffilter1)
abuelas = abuelas.map(lambda t: (t[1]))
personas = personas.filter(ffilter2)
personas = personas.map(lambda t: (t[2], (t[0], t[1])))

print('IDs de superabuelas')
print(abuelas.collect())
print("______________________________________________________")

for abuelaID in abuelas.collect():
    ancestros = sc.parallelize([[abuelaID, abuelaID]])
    ancestros = ancestros.map(lambda t: (t[0], (t[1])))
    abuelaBC = sc.broadcast([abuelaID, 0])
    print("ABUELA: " + abuelaID)

    while True:
        descendientes = ancestros.join(personas)
        cantHijos = sum(descendientes.countByKey().values())
        abuelaBC.value[1] = abuelaBC.value[1] + cantHijos
        if (not descendientes.collect()):
            break
        else:        
            ancestros = descendientes.map(lambda t: (t[1][1][1], (t[0])))

    print(abuelaBC.value)
    print("______________________________________________________")
