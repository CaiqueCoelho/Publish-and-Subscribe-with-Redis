#!/usr/bin/python
# -*- coding: utf-8 -*-

#DADOS ALUNO E DISCIPLINA
# Aluno: Caique de Paula Figueiredo Coelho
# RGA: 201319040152
# UFMS: Ciência da Computação
# Sisteas Distribuidps - 2017.2
# Trabalho 1: Pub/Sub com redis

#PREPARACAO DA MAQUINA PARA EXECUCAO
#Intale o redis-py com: $ sudo pip install redis
#Instale o redis-server com: $ sudo apt-get install redis-server
#Certifique-se que a porta 6379 esta disponivel, pois e a porta padrao para uso do redis-py
#Certifique-se que o seu comando python no terminal executa o python2

#COMO EXECUTAR
# No terminal, estando no diretório onde este arquivo se encontra 
# informe a seguinte linhha de comando: python publish.py 1000 5 20 8 10 2 3
# ou: python publish.py 1000 20 5 8 10 2 3

#PARA IMPLEMENTACOES FUTURAS no T2:
# 1. Nao foi possivel respeitar os parametros de tempo informado
# 2. Nao foi possivel recuperar a lista e quantidade de assinantes, que nao receberam as mensagens cujos 
#    topicos assinou, e que foram publicadas pela publicadora que quebrou
# 3. Nao foi possivel recuperar Lista e quantidade de mensagens publicadas, com topicos da assinante que quebrou, 
#    que nao foram recebidas pela assinante antes desta quebrar

import redis
from datetime import datetime
import time
from time import sleep
import threading
import sys
import random


if len(sys.argv)!=8:
	print "Parametros de linha INCORRETOS, Use:"
	print sys.argv[0], " seed(int) npubs(int) nsubs(int) ntops(int) rounds(int) interpub(int) delta(int)"
	exit()

seed = int(sys.argv[1]) #No estado inicial do sistema, a semente para geração de números pseudoaleatorios
npubs = int(sys.argv[2]) #Ha npubs publicadoras, identificadas com idpub, 0 ≤ idpub ≤ npubs − 1
nsubs = int(sys.argv[3]) #Ha nsubs assinantes, identificadas com idsub, 0 ≤ idsub ≤ nsubs − 1.
ntops = int(sys.argv[4]) #As publicadoras e assinantes sao threads que relacionam-se por ntops tópicos, identificados com idtop, 0 ≤ idtop ≤ ntops − 1.
rounds = int(sys.argv[5]) #Deve haver exatamente rounds rodadas de publicacoes, identificadas com idround, 0 ≤ idround ≤ round − 1
interpub = int(sys.argv[6])
delta = int(sys.argv[7])
int_number = 1
#timeIdround = idround * interpub #Cada rodada idround ocorre no instante de tempo idround × interpub segundos

myredis = redis.client.StrictRedis()
random.seed(seed) #inicia a semente dos número pseudo randômicos

lista_idtop = []
for idtop in range(0, ntops):
	lista_idtop.append(idtop)


thread_pub_lista = []
thread_sub_lista = []


t0 = 0
t0_system = 0
tf = delta + rounds * interpub


lista_idtop_pub_global = []
lista_idtop_sub_global = {}


def pub(myredis, idpub, int_number, rounds, ntops):
	
	t0_local = int(round(time.time() * 1000))
	global t0_system


	lista_idtop_pub_local = []
	lista_idtop_pub_local.append(idpub)
	global lista_idtop_pub_global

	for idround in range(0, rounds):
		idtop = random.randrange(0, ntops)
		lista_idtop_pub_local.append(idtop)
		
		msgPub ="From publisher " +str(idpub) + " " + str(idtop) + ": Msg " + str(int_number)
		int_number = int_number + 1
		myredis.publish(str(idtop), msgPub)

		ti = int(round(time.time() * 1000)) - t0_system

		print str(ti) + "(ti): Publicadora " +str(idpub) +" " + msgPub

		time.sleep(1)

		if(idround == rounds - 1):
			tf = int(round(time.time() * 1000)) - int(t0_system)
			
			print(str(ti) + "(ti): Quebra de publicadora " +str(idpub) + ": " + msgPub + 
				" " +str(lista_idtop_pub_local))

			lista_idtop_pub_global.append(lista_idtop_pub_local)

def sub(myredis, idsub, rounds, channels):

	id = idsub * 10
	t0_local = int(round(time.time() * 1000))
	global t0_system


	lista_idtop_sub_local = []
	global lista_idtop_pub_global


	sub = myredis.pubsub()
	sub.subscribe(channels)

	ti = int(round(time.time() * 1000)) - t0_system

	for m in sub.listen():
		if "From publisher" in str(m['data']):
			print str(ti) + "(ti): Assinante " +str(idsub) + ": " + str(m['data'])
			n =random.random()

	#PAUSE = True

	#while PAUSE:
		#print("Waiting for message...")
	#	message = sub.get_message()
		#print(type(sub.get_message))
	#	if "From publisher" in str(message):
	#		ti = int(round(time.time() * 1000)) - t0_local
	#		print str(ti) + "(ti): Quebra de assinante " +str(idsub) + ": " + message['data'] + " " + str(channels)
	#		PAUSE = False

	#exit()


for idpub in range(0, npubs):	
	thread_pub_lista.append(threading.Thread(target=pub, args=(myredis, idpub, int_number, rounds, ntops)))
	int_number = int_number + 1 + rounds


for idsub in range(0, nsubs):
	thread_sub_lista.append(threading.Thread(target=sub, args=(myredis, idsub, rounds, lista_idtop)))

print str(t0) + "(t0): Início da simulação"
t0_system = int(round(time.time() * 1000))

for thread_pub in thread_pub_lista:
	#thread_pub.setDaemon(True)
	thread_pub.start()
	#thread_pub.join()

time.sleep(delta)

for thread_sub in thread_sub_lista:
	thread_sub.setDaemon(True)
	thread_sub.start()
	#thread_sub.join()

time.sleep(int(tf - 15))

tf_system = int(round(time.time() * 1000)) - t0_system
print str(tf_system) + "(tf): Fim da simulação"

#print len(lista_idtop_sub_global)
#print lista_idtop_sub_global

for response in lista_idtop_pub_global:

	for index in range(len(response)):
		if index == 0:
			msg_1 = "Publicadora " + str(response[index]) + " publicou topicos: "

		elif index == 1:
			msg_1 = str(msg_1) + " " + str(response[index])

		else:
			msg_1 = str(msg_1) + ", " + str(response[index])

	print msg_1


for idsub in range(0, nsubs):

	print "Assinante " +str(idsub) + " assinou topicos: " + str(lista_idtop)


#print(type(lista_idtop_sub_global))

#for idsub in range(0, nsubs):
#	idtop_dont_receive = []
#	idtop_receive = []
#	print "IDSUBBBBBBBBBBBBBBBBBB " +str(idsub)
#	for assinou in range(len(lista_idtop_sub_global)):
#		print "Idsub " + str(lista_idtop_sub_global.values()[assinou][0])
#		print "Idtop " + str(lista_idtop_sub_global.values()[assinou][1])
#		if(lista_idtop_sub_global.values()[assinou][0] == idsub):
#			idtop_receive.append(lista_idtop_sub_global.values()[assinou][1])
			#print "idsub_assinou " + str(lista_idtop_sub_global.keys()[assinou])

	#print "idtop_receive" + str(idtop_receive)

#	for idtop in lista_idtop:
#		if idtop not in idtop_receive:
#			idtop_dont_receive.append(idtop)
	
	#print("Assinante não recebeu os topicos: " + str(idtop_dont_receive))

#print lista_idtop_pub
#1000 5 20 8 10 2 3