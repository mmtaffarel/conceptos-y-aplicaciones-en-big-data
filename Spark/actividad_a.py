#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Nov 18 15:12:19 2021

@author: Marcelo Taffarel
"""
"""
Actividad a.

Dado los DNI de dos individuos determinar si son primos (dos individuos son primos si tienen la misma abuela)
"""

"""
La idea consiste en hacer un map con el split \t, luego un filter para obtener una rdd solo con los documentos
ingresados y por ultimo un join entre esta rdd y la rdd original pero por los ids de madre. Previamente, antes 
de hacer el join, hay que verificar que ambos DNI no tengan la misma madre ya que si es así entonces son hermanos
y ya no se requiere verificar si son primos.
"""
import os, sys

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

'''
Ingreso de los DNI por teclado
'''
dni1 = input("Ingrese el primer DNI: ")
dni2 = input("Ingrese el segundo DNI: ")

personas = sc.textFile(fileInputPath)
bc = sc.broadcast([dni1, dni2])

def ffilter1(t):
    if(t[1] in bc.value):
        if t[2] != 'None':
            return t

personas = personas.map(lambda line: line.split("\t"))
dnis = personas.filter(ffilter1)

if(dnis.count() < 2):
    print("Alguno de los DNIs ingresados no tiene madre o no está en la BBDD")
    print(dnis.collect())
    sys.exit()

dnis = dnis.map(lambda t: (t[2], (t[0], t[1])))
personas = personas.map(lambda t: (t[1], (t[0], t[2])))

madres = dnis.join(personas)

if(madres.count() < 2):
    print("Alguno de los DNIs ingresados no tiene madre")
    sys.exit()
else:
    res = madres.collect()
    print("RESULTADO:")
    if(res[0][0] == res[1][0]):
        print("Los DNI ingresados tienen la misma madre por lo tanto son hermanos")
        sys.exit()
    else:
        if(res[0][1][1][1] == res[1][1][1][1]):
            print("Los DNI ingresados son primos")
        else:
            print("Los DNI ingresados no son primos")            

    
