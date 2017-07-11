//#!/usr/bin/python
###########################################################################
#	Filename   : RS_server_withQueue.py (the version 3 of RS_server)
#	Author     : Benden
#	Description: Receice real-time traffic data(GPS) from JiaoWei and
#		     then send the data to other App-client. the local host
#		     will keep a backup copy of the data.
###########################################################################

##########################################################################
#
#	change log
#		1. add a global queue(FIFO) to buffer the receive msg and create
#		a new Thread to send msg from the queue
#		2. add queue in every sendport handle thread for each retransmit
#		3. add ip verify for client's app, need config file and live add or del
#		4. limit the queue_buf size in every datasendport_handle thread to 1000000 msg
#		5. add a monitor command "recv_speed\r\n" - count the average speed of coming msg in second
#
#		6.dismiss the recv of gps,warning,operations msg/2014.4.17
##########################################################################
###########################################################################
#
#	Usage:TODO:XXXXXXXXXXXXXXXXXXX
#
###########################################################################

import threading
import traceback
import socket
import sys
import time
import logging
import os
import struct
import Queue
import re
###########################################################################
#	Global Variables
###########################################################################
G_FIFO_queue = Queue.Queue()
G_AppClientList_all = []	# used for verifying the app client who wants to recv all types msg
G_AppClientList_all_lock = threading.Lock()
G_AdminList = []		# used for verifying the one who is connected to monitor whether has the admin privilge
G_AdminList_lock = threading.Lock()

G_connected_IPs = {}		# used for count the times that one IP connected the the send port
G_connected_IPs_lock = threading.Lock()
G_max_count = 3
send_max_conn = 50     ## used for set the most concurrent connections of the send port
recv_max_conn = 1      # used for set the most concurrent connections of the recv port 
monitor_max_conn = 10  #used for set the most concurrent connections of the recv port
###########################################################################
#	Def Functions
###########################################################################

##
# Load config.ini file
##
def loadConfig(configfile):	# in this version only load the authencation list
	import ConfigParser
	config = ConfigParser.ConfigParser()
	config.read(configfile)
	admin_list = config.get("Authentication","admin_list")
	admin_list = admin_list.split(":")
	all_list = config.get("Authentication","all_list")
	all_list = all_list.split(":")
	client_pwd = config.get("Authentication","client_pwd")	
	monitor_pwd = config.get("Authentication","monitor_pwd")
	recv_port = config.getint("ServerInfo","recv_port")
	send_port = config.getint("ServerInfo","send_port")
	monitoring_port = config.getint("ServerInfo","monitoring_port")
	recv_max_conn = config.getint("ConnectionLimit","recv_max_conn")
	send_max_conn = config.getint("ConnectionLimit","send_max_conn")
	monitor_max_conn = config.getint("ConnectionLimit","monitor_max_conn")
	G_max_count = config.getint("ConnectionLimit","G_max_count")
	DS_pattern = config.get("DataSources","DS_pattern")
	DS_unit = config.get("DataSources","DS_unit")
	recv_function = config.get("RecvModule","recv_function")
	return admin_list,all_list,client_pwd,monitor_pwd,recv_port,send_port,monitoring_port,recv_max_conn,send_max_conn,monitor_max_conn,G_max_count,DS_pattern,DS_unit,recv_function
##
# Init HDlogger - for local file logger
## 
def initHDlog(logfile):
	import logging
	logger = logging.getLogger()
	hdlr = logging.FileHandler(logfile)
	formatter = logging.Formatter('%(asctime)s  %(levelname)s  %(message)s')
	hdlr.setFormatter(formatter)
	logger.addHandler(hdlr)
	logger.setLevel(logging.INFO)
	return logger
# initHDlog END

##
# check a IP address is an IP from DataSource or not
##
def isLegalDS(IP,DS_pattern):
#	return True
	match = re.search(DS_pattern,IP)
	if match:
		return True
	else:
		return False
##
# check a IP address is a legal IP address or not
##
def isLegalIP(IP):
	IP_pattern = '^(([01]?\d\d?|2[0-4]\d|25[0-5])\.){3}([01]?\d\d?|2[0-4]\d|25[0-5])$'
	match = re.search(IP_pattern,IP)
	if match:
		return True
	else:
		return False
##
# G_connected_IPs operation for threads safely
##
def countAIP(IP):
	flag = False
	if(False==isLegalIP(IP)):
		return False
	G_connected_IPs_lock.acquire()
	if IP in G_connected_IPs:
		G_connected_IPs[IP] = G_connected_IPs[IP] + 1
		flag = True
	else:
		G_connected_IPs[IP] = 1
		flag = True
	G_connected_IPs_lock.release()
	return flag
def uncountAIP(IP):
	flag = False
	if(False==isLegalIP(IP)):
		return False
	G_connected_IPs_lock.acquire()
	if IP in G_connected_IPs:
		G_connected_IPs[IP] = G_connected_IPs[IP] - 1
		if( G_connected_IPs[IP]==0 ):
			G_connected_IPs.pop(IP)
		flag = True
	G_connected_IPs_lock.release()
	return flag
##
# list operations for threads safely
##
def addToList(g_list,IP):
	if(False==isLegalIP(IP)):
		return False
	g_list_lock = ''
	if(g_list==G_AppClientList_all):
		g_list_lock = G_AppClientList_all_lock
	elif(g_list==G_AdminList):
		g_list_lock = G_AdminList_lock

	g_list_lock.acquire()
	for ip_addr in g_list:
		if IP==ip_addr:
			g_list_lock.release()
			return True
	g_list.append(IP)
	g_list_lock.release()
	return True
def delFromList(g_list,IP):
	if(False==isLegalIP(IP)):
		return False
	g_list_lock = ''
	if(g_list==G_AppClientList_all):
		g_list_lock = G_AppClientList_all_lock
	elif(g_list==G_AdminList):
		g_list_lock = G_AdminList_lock

	flag = True
	g_list_lock.acquire()
	for ip_addr in g_list:
		if IP==ip_addr:
			flag = False
	if not flag:
		flag = True
		g_list.remove(IP)
	g_list_lock.release()
	return flag
def isInList(g_list,IP):
	flag = False
	for ip_addr in g_list:
		if IP==ip_addr:
			flag = True
	return flag

###########################################################################
#	Def Threads Class
###########################################################################

##
# This Thread class is used for handling every connection comming from JIAOWEI
#
#
##
class Thread_datarecvport_handle(threading.Thread):
	def __init__(self,clientsock,clientaddr,logger):
		threading.Thread.__init__(self)
		self.clientsock = clientsock
		#self.clientsockfd = clientsock.makefile('rwb')
		self.clientaddr = clientaddr
		self.logger = logger
		self.parent = ''
	def setParentThread(self,parent):
		self.parent = parent		# For this program situation parent is recvlisten_thread
	def getSendList(self):
		if(self.parent):
			return self.parent.sendlist
		return []

	def run(self):
		self.recvlisten_thread = self.parent
		global DS_unit
		# Send message to client first - due to the protocol designed by ST
		init_buf = Recv_module.initMessage()
		if init_buf :
			self.clientsock.sendall(init_buf)
		while 1:
			try:
				self.action = -1
				self.buf, self.action = Recv_module.recvAMessage(self.clientsock,self.logger,self.recvlisten_thread)
				# actions:
				#	0 - close connection
				#	1 - distribute
				#	2 - reply client
				#	3 - ignore
				if( 0==self.action ):
					# the connection is already close
					self.logger.info('Thread_datarecvport_handle: Connection closed by DataSoures Client-%s' %DS_unit)
					print 'Thread_datarecvport_handle: Connection closed  by DataSoures Client-%s' %DS_unit
					self.recvlisten_thread.handle_unit=''
					self.recvlisten_thread.setConnectionCloseTime()
					self.clientsock.close()
					break
				elif ( 1==self.action ):
					if(self.buf==''):
                                                print "ignorn because buf is null"
                                                continue
					self.recvlisten_thread.addTotalCount()
					self.recvlisten_thread.addDayTotalCount()
					global G_FIFO_queue
					G_FIFO_queue.put(self.buf)
				elif ( 2==self.action ):
					print 'send a message to DataSource client'
					self.clientsock.sendall(self.buf)
				elif ( 3==self.action ):
					print 'ignore a message'
					continue
				else:
					print 'error deal with a message'
					self.logger.warning('Thread_datarecvport_handle: recvAMessage didnt return a valid action type')
					continue
			except socket.timeout:
				self.logger.warning('Thread_datarecvport_handle: Connection timeout, the connection from %s will be disconnect' %DS_unit)
				print 'Thread_datarecvport_handle: Connection timeout, the connection from %s will be disconnect' %DS_unit
				#self.clientsock.shutdown(socket.SHUT_RDWR)
				self.clientsock.close()
				self.recvlisten_thread.handle_unit=''
				self.recvlisten_thread.setConnectionCloseTime()
				#sys.exit(1)
				return
			except Exception,ex:
				self.logger.critical('Thread_datarecvport_handle: Error occurred while recv data,the connection from %s will be disconnect' %DS_unit)
				print 'Thread_datarecvport_handle: Error occurred,the connection from %s will be disconnect' %DS_unit
				traceback.print_exc()
				self.logger.critical(ex)
				#self.clientsock.shutdown(socket.SHUT_RDWR)
				self.clientsock.close()
				self.recvlisten_thread.handle_unit=''
				self.recvlisten_thread.setConnectionCloseTime()
				#sys.exit(1)
				return

##
# This Thread class is used for handling every connection comming from XJY's APP
#
#
##
class Thread_datasendport_handle(threading.Thread):
        def __init__(self,clientsock,clientaddr,logger):
		threading.Thread.__init__(self)
                self.clientsock = clientsock
		self.clientsockfd = clientsock.makefile('rw')	# make socket to be used as file
                self.clientaddr = clientaddr
                self.logger = logger
		self.parent = ''
#		self.registerType = 0				# 0 for none, 1-all, 2-gps,3-warning,4-operations

		self.queue_buf = Queue.Queue()
	def setParentThread(self,parent):
		self.parent = parent
	def pushToQueue(self,message):
		self.queue_buf.put(message)

	def getInfo(self):
		buf = self.clientaddr
		queuesize = self.queue_buf.qsize()
		result = "%s queue_buf size: %s # " %(buf,queuesize)
		return result

	def registerAll(self,sendthread):
		if(self.parent):
			return self.parent.registerAll(sendthread)
		return False
	def unregisterAll(self,sendthread):
		if(self.parent):
			return self.parent.unregisterAll(sendthread)
		return False
	def clientsockClose(self,clientsock):
		try:
			clientsock.close()
                except Exception,ex:
			self.logger.critical("Thread_datasendport_handle: An Error happend when self.clientsock.close()")
                        traceback.print_exc()
			self.logger.critical(ex)
                        #sys.exit(1)
        def run(self):
		# check whether in IP list
		if(False==isInList(G_AppClientList_all,self.clientaddr[0])):
			self.logger.info("Thread_datasendport_handle: Send data connection %s  will be closed becasue client is not in IP list." %self.clientaddr[0])
			print "Thread_datasendport_handle: Send data connection %s  will be closed because client is not in IP list." %self.clientaddr[0]
			self.clientsock.sendall("connection will be closed because you are not in IP list.\n")
			#self.clientsock.shutdown(socket.SHUT_RDWR)
			self.clientsockClose(self.clientsock)  #change
			return
		# verify client's password
		verify_flag = False
		global client_pwd
		try:
			self.buf = self.clientsockfd.readline()
#			self.needType = self.clientsockfd.readline()
		except socket.timeout:
			self.logger.warning("Thread_datasendport_handle: Senddata connection timeout--. %s connection will be closed" %self.clientaddr[0])
			print "Thread_datasendport_handle: Senddata connection timeout. %s connection will be closed" %self.clientaddr[0]
			#self.clientsock.shutdown(socket.SHUT_RDWR)
			self.clientsockClose(self.clientsock)  #change
			return
		except Exception,ex:
			self.logger.critical("Thread_datasendport_handle:An ERROR happened. Connection has been closed by %s" %self.clientaddr[0])
			print "Thread_datasendport_handle: Connection has been closed by %s" %self.clientaddr[0]
			#self.clientsock.shutdown(socket.SHUT_RDWR)
			self.clientsockClose(self.clientsock)  #change
			traceback.print_exc()
		#	self.logger.critical(ex)
			#sys.exit(1) #change
			return
		
		if(0==cmp(self.buf,client_pwd)): # maybe add 3 more list to separate different msg in order to re-transmit them separatl
			if(True==isInList(G_AppClientList_all,self.clientaddr[0])):
				if(True==self.registerAll(threading.currentThread())):
					verify_flag = True
				else:
					print 'an error happend when adding item to SendList.'
		if(not verify_flag):
			self.logger.info("Thread_datasendport_handle: Senddata %s can not be verify, RS_server will close the connection." %(self.clientaddr[0]))
			print "Senddata %s can not be verify, RS_server will close the connection." %(self.clientaddr[0])
			try:
				self.clientsock.sendall("password or needTpye can not be verify, RS_server will close the connection.\n")
				#self.clientsock.shutdown(socket.SHUT_RDWR)
				self.clientsock.close()
				return
			except Exception,ex:
				self.logger.critical("Thread_datasendport_handle: An Error happend when self.clientsock.close()")
				traceback.print_exc()
				self.logger.critical(ex)
				#sys.exit(1)
				return #change
		# the app client is verified and add to some sendlist

		# now count the connection IP
		if(False==countAIP(self.clientaddr[0])):
			self.logger.info("Thread_datasendport_handle: cannot count IP %s" % self.clientaddr[0])
			print "Thread_datasendport_handle cannot count IP %s" % self.clientaddr[0]
			self.clientsockClose(self.clientsock)  #change
			return
		while 1:
			if(1000000<self.queue_buf.qsize()):
				self.logger.warning("client %s:%s receive data too slowly, here will drop a large count of data about 1000000 messages" %(self.clientaddr[0],self.clientaddr[1]))
				print "client %s:%s receive data too slowly, here will drop a large count of data about 1000000 messages" %(self.clientaddr[0],self.clientaddr[1])
				while(self.queue_buf.qsize()>50000):
					self.queue_buf.get()
			msg = self.queue_buf.get()	# if not msg in the queue will block here
			if(not msg):
				print 'send msg to %s get an unexpect message,here will break the getting msg while() loop' % self.clientaddr[0]
				self.logger.warning('Thread_datasendport_handle: send msg to %s get an unexpect message,here will break the while() loop' %self.clientaddr[0])

				self.unregisterAll(threading.currentThread())

				if(False==uncountAIP(self.clientaddr[0])):
					self.logger.info("Thread_datasendport_handle: cannot uncount IP %s" % self.clientaddr[0])
					print "Thread_datasendport_handle cannot uncount IP %s" % self.clientaddr[0]
				self.logger.info("Thread_datasendport_handle uncount IP %s successed." % self.clientaddr[0])
				#self.clientsock.shutdown(socket.SHUT_RDWR)
				self.clientsockClose(self.clientsock)  #change
				break
			try:
				while 1:
					tmp_len = self.clientsock.send(msg)
					if(tmp_len < len(msg)):
						self.logger.info("Thread_datasendport_handle hasn't sent a total msg, will resend the left %d bytes" % (len(msg)-tmp_len))
						print "Thread_datasendport_handle hasn't sent a total msg, will resend the left %d bytes" % (len(msg)-tmp_len)
						msg = msg[tmp_len:]
						continue
					if(tmp_len<=0):
						self.logger.critical("Thread_datasendport_handle unknow error occurred, send is len: %s" %tmp_len)
						print "Thread_datasendport_handle unknow error occurred, send is len: %s" %tmp_len
						self.clientsockClose(self.clientsock)  #change
						#sys.exit(1)
					break
			except socket.timeout:
				self.logger.info('Thread_datasendport_handle: socket timeout when sending data to %s:%s, connection will be close!' %(self.clientaddr[0],self.clientaddr[1]))
				print'Thread_datasendport_handle socket timeout when sending data to %s:%s, connection will be close!' %(self.clientaddr[0],self.clientaddr[1])
				
				self.unregisterAll(threading.currentThread())

				if(False==uncountAIP(self.clientaddr[0])):
					self.logger.info("Thread_datasendport_handle cannot uncount IP %s" % self.clientaddr[0])
					print "Thread_datasendport_handle cannot uncount IP %s" % self.clientaddr[0]
				self.logger.info("Thread_datasendport_handle uncount IP %s successed." % self.clientaddr[0])
				#self.clientsock.shutdown(socket.SHUT_RDWR)
				self.clientsockClose(self.clientsock)  #change
				#sys.exit(1)
				return
			except Exception,ex:
				self.logger.critical("Thread_datasendport_handle:Error occurred. Socket closed by client %s:%s" %(self.clientaddr[0],self.clientaddr[1]))
				print "Thread_datasendport_handle:An ERROR happened. Socket closed by client %s:%s" %(self.clientaddr[0],self.clientaddr[1])				
				traceback.print_exc()
				self.logger.critical(ex)
				self.unregisterAll(threading.currentThread())

				if(False==uncountAIP(self.clientaddr[0])):
					self.logger.info("Thread_datasendport_handle cannot uncount IP %s" % self.clientaddr[0])
					print "Thread_datasendport_handle cannot uncount IP %s" % self.clientaddr[0]
				self.logger.info("Thread_datasendport_handle uncount IP %s successed." % self.clientaddr[0])
				self.clientsockClose(self.clientsock)  #change
				#sys.exit(1)
				return #change

		print "datasendport_handle thread should not come here, please check your coed!!"
		self.logger.critical("datasendport_handle thread should not come here, please check your coed!!")
		return

##
# This Thread class is used for getting a message from Global queue and dispatch to every send handle thread which was already registered
#	
#
##
class Thread_dispatcher(threading.Thread):
        def __init__(self,parent,logger):
                threading.Thread.__init__(self)
                self.logger = logger
                self.parent = parent

	        self.sendlist_all = []
                self.sendlist_all_lock = threading.Lock()

        def setParentThread(self,parent):
                self.parent = parent			# for here the parent is Thread_datasendport_listen
        def registerAll(self,sendthread):
		flag = False
		self.sendlist_all_lock.acquire()
                for thread in self.sendlist_all:
                        if(thread==sendthread):
                                flag = True
				self.logger.info('thread %s is already in the sendlist_all list' %(thread))
				print 'thread %s is already in the sendlist_all list' %(thread)
		if(not flag):
			flag = True
			self.sendlist_all.append(sendthread)
			self.logger.info('thread %s is added sendlist_all' %(sendthread))
			print 'thread %s is added to sendlist_all' %(sendthread)
		self.sendlist_all_lock.release()
		return flag
	def unregisterAll(self,sendthread):
		flag = True
		self.sendlist_all_lock.acquire()
		for thread in self.sendlist_all:
			if(thread==sendthread):
				flag = False
		if(not flag):
			self.sendlist_all.remove(sendthread)
			self.logger.info('thread %s is removed from sendlist_all' %(sendthread))
			print 'thread %s is removed from sendlist_all' %(sendthread)
		self.sendlist_all_lock.release()
		return True


        def run(self):
                global G_FIFO_queue
		while 1:
			msg = G_FIFO_queue.get()	# if no msg in queue will block here
			# this sendlist is sendlist_all
			for thread in self.sendlist_all:
				try:
					thread.pushToQueue(msg)
				except Exception,ex:
					self.logger.critical("Thread_dispatcher can not call pushToQueue function in thread %s" %(thread))
					print "Thread_dispatcher can not call pushToQueue function in thread %s" %(thread)
					traceback.print_exc()
					self.logger.critical(ex)
					self.unregisterAll(thread)

		print 'Thread_dispatcher should not reach here, please check your code!!'

##
# This Thread class is used for handling every connection comming from monitoring APPs
#
#
##
class Thread_monitoringport_handle(threading.Thread):	
	def __init__(self,clientsock,clientaddr,logger):
		threading.Thread.__init__(self)
		self.clientsock = clientsock
		self.clientsockfd = clientsock.makefile('rw')
		self.clientaddr = clientaddr
		self.logger = logger
		self.parent = ''
	def setParentThread(self,parent):
		self.parent = parent	# for this situation, parent's parent is the Thread_datarecvport_listen 
	def clientsockClose(self,clientsock):
                try:
                        clientsock.close()
                except Exception,ex:
			self.logger.critical("Thread_Monitoring_handle: An Error happend when self.clientsock.close()")
                        traceback.print_exc()
			self.logger.critical(ex)
                        #sys.exit(1)		
	def recvBuf(self,clientsockfd):
                try:
                        msg = clientsockfd.readline()
			if( not msg):
				# the connection is already close
				self.logger.info("Monitoring connection closed by monitor %s." %self.clientaddr[0])
	                        print "Monitoring connection closed by monitor %s." %self.clientaddr[0]
				self.clientsockClose(self.clientsock) #change
				return			
                except socket.timeout:
                        self.logger.info("Monitoring connection timeout. %s connection will be closed" %self.clientaddr[0])
			print "Monitoring connection timeout. %s connection will be closed" %self.clientaddr[0]
			#self.clientsock.shutdown(socket.SHUT_RDWR)
                        self.clientsockClose(self.clientsock) #change
                        return
                except Exception,ex:
			self.logger.critical("Thread_Monitoring_handle Connection has been closed by %s" %self.clientaddr[0])
                        traceback.print_exc()
			self.logger.critical(ex)
			self.clientsockClose(self.clientsock) #change
                        #sys.exit(1)
			return
		return msg
#############################################################################		
	def run(self):
		self.recvlisten_thread = self.parent.parent
		if(not self.recvlisten_thread):
			self.logger.error("Monitoring thread cannot get recvlisten_thread handle" %self.recvlisten_thread)
			print "Monitoring thread cannot get recvlisten_thread handle %s" %self.recvlisten_thread
			self.clientsockClose(self.clientsock) #change
			self.clientsockfd.close() #change
			sys.exit(1) #change
		global G_AdminList,G_AppClientList_all,client_pwd,monitor_pwd,recv_port,send_port,monitoring_port,recv_max_conn,send_max_conn,monitor_max_conn,DS_pattern,DS_unit,recv_function,G_max_count
		# verify monitor's password
		self.buf=self.recvBuf(self.clientsockfd)
		#print self.buf
		#self.logger.info(self.buf)
		if(not self.buf):
			#the connection is already close
			self.clientsockfd.close() #change
			return
		#print monitor_pwd
                if(0!=cmp(self.buf,monitor_pwd)):
			# cannot be verify
                	self.logger.info("Monitoring %s can not be verify, RS_server will close the connection" %self.clientaddr[0])
                	print "Monitoring %s can not be verify, RS_server will close the connection" %self.clientaddr[0]
			self.clientsockClose(self.clientsock) #change
			self.clientsockfd.close() #change
                	return
		while 1:
			self.buf=self.recvBuf(self.clientsockfd)
			if(not self.buf):
				#the connection is already close
			#	print "read %s monitor's command failed" %self.clientaddr[0]
				break    #change
			#print self.buf	
			if(0==cmp(self.buf,"total_count\r\n")):
				self.sendbuf = self.recvlisten_thread.getTotalCount()
				self.clientsockfd.write("%s\n" %self.sendbuf)
				self.clientsockfd.write(end_flag)
				self.clientsockfd.flush()
			elif(0==cmp(self.buf,"day_total_count\r\n")):
				self.sendbuf = self.recvlisten_thread.getDayTotalCount()
                                self.clientsockfd.write("%s\n" %self.sendbuf)
				self.clientsockfd.write(end_flag)
                                self.clientsockfd.flush()
			elif(0==cmp(self.buf,"recv_speed\r\n")):
				tmp_count = self.recvlisten_thread.getDayTotalCount()
				time.sleep(1)
				self.sendbuf = self.recvlisten_thread.getDayTotalCount() - tmp_count
				self.clientsockfd.write("%s\n" %self.sendbuf)
				self.clientsockfd.write(end_flag)
				self.clientsockfd.flush()
			elif(0==cmp(self.buf,"misunderstand_count\r\n")):
				self.sendbuf = self.recvlisten_thread.getMisunderstandCount()
                                self.clientsockfd.write("%s\n" %self.sendbuf)
				self.clientsockfd.write(end_flag)
                                self.clientsockfd.flush()
			elif(0==cmp(self.buf,"day_misunderstand_count\r\n")):
				self.sendbuf = self.recvlisten_thread.getDayMisunderstandCount()
                                self.clientsockfd.write("%s\n" %self.sendbuf)
				self.clientsockfd.write(end_flag)
                                self.clientsockfd.flush()
			elif(0==cmp(self.buf,"transmit_list_all\r\n")):
				self.sendbuf = ''
				for thread in self.recvlisten_thread.sendlisten_thread.dispatcher_thread.sendlist_all:
					self.sendbuf = self.sendbuf + thread.getInfo()
                                self.clientsockfd.write("%s\n" %self.sendbuf)
				self.clientsockfd.write(end_flag)
                                self.clientsockfd.flush()
			elif(0==cmp(self.buf,"connection_timebuckets\r\n")):
				timebuckets = self.recvlisten_thread.getConnectionTimebuckets()
				for timebucket in timebuckets:
					#self.sendbuf = time.asctime(timebucket[0])
					self.sendbuf = time.strftime("%Y-%m-%d %H:%M:%S",timebucket[0])
					if(timebucket[1] != 0 ):
						#self.sendbuf = self.sendbuf + ' --- ' + time.asctime(timebucket[1])
						self.sendbuf = self.sendbuf + ' --- ' + time.strftime("%Y-%m-%d %H:%M:%S",timebucket[1])
					else:
						self.sendbuf = self.sendbuf + ' --- ' + "TILL NOW"
                                	self.clientsockfd.write("%s\n" %self.sendbuf)
				self.clientsockfd.write(end_flag)
                                self.clientsockfd.flush()
			elif(0==cmp(self.buf,"server_starttime\r\n")):
				self.sendbuf = time.strftime("%Y-%m-%d %H:%M:%S",self.recvlisten_thread.getServerStarttime())
                                self.clientsockfd.write("%s\n" %self.sendbuf)
				self.clientsockfd.write(end_flag)
                                self.clientsockfd.flush()
			elif(0==cmp(self.buf,"queue_size\r\n")):
				self.sendbuf = G_FIFO_queue.qsize()
                                self.clientsockfd.write("%s\n" %self.sendbuf)
				self.clientsockfd.write(end_flag)
                                self.clientsockfd.flush()
			elif(0==cmp(self.buf,"authentication_lists\r\n")):
				self.sendbuf = 'admin_list:%s, all_list:%s' %(G_AdminList,G_AppClientList_all)
				self.clientsockfd.write("%s\n" %self.sendbuf)
				self.clientsockfd.write(end_flag)
				self.clientsockfd.flush()
			elif(0==cmp(self.buf,"G_max_count\r\n")):
				self.sendbuf = 'G_max_count = %d' %G_max_count
				self.clientsockfd.write("%s\n" %self.sendbuf)
                                self.clientsockfd.write(end_flag)
                                self.clientsockfd.flush()
			elif(0==cmp(self.buf,"admin\r\n")):
				if(False==isInList(G_AdminList,self.clientaddr[0])):
					self.clientsock.sendall("You don't have privilge to do this!!\n")
					continue
				self.clientsock.sendall("[operation]:[object]:[value]\n")
				#self.buf = self.clientsockfd.readline()
				self.buf=self.recvBuf(self.clientsockfd)
				if(not self.buf):
					#the connection is already close
					break
				self.buf = self.buf.rstrip()
				arg = self.buf.split(":")
				if(len(arg)!=3):
					continue
				self.sendbuf=''
				cmd_list = ''
				value = G_max_count
				if(arg[0]=='set'):
					if(arg[1]=='G_max_count'):
						try:
							value = int(arg[2])
							G_max_count = value
							self.sendbuf = 'True'
						except:
							self.sendbuf = 'False'
				elif(arg[0]=='reload_config'):
					admin_list,all_list,client_pwd,monitor_pwd,recv_port,send_port,monitoring_port,recv_max_conn,send_max_conn,monitor_max_conn,G_max_count,DS_pattern,DS_unit,recv_function = loadConfig("config.ini")
					G_AppClientList_all = []     
					G_AdminList = [] 
					monitor_pwd='%s\r\n' %monitor_pwd
					client_pwd='%s\r\n' %client_pwd               
					for item in admin_list:
        					addToList(G_AdminList,item)
					for item in all_list:
        					addToList(G_AppClientList_all,item)
					self.logger.info("%s has reloaded the config." %self.clientaddr[0])
					self.sendbuf = 'True'
				elif(arg[0]=='add'):
                                	if(arg[1]=='admin'):
                                        	cmd_list = G_AdminList
                                	elif(arg[1]=='all'):
                                        	cmd_list = G_AppClientList_all
					self.sendbuf = addToList(cmd_list,arg[2])
					self.logger.info("%s has added a new ip to %s" %(self.clientaddr[0],arg[1]))
				elif(arg[0]=='del'):
                                        if(arg[1]=='admin'):
                                                cmd_list = G_AdminList
                                        elif(arg[1]=='all'):
                                                cmd_list = G_AppClientList_all
					self.sendbuf = delFromList(cmd_list,arg[2])
					self.logger.info("%s has del a new ip from %s" %(self.clientaddr[0],arg[1]))

				else:
					continue
				self.clientsockfd.write('%s\n' %self.sendbuf)
				self.clientsockfd.write(end_flag)
				self.clientsockfd.flush()
			elif(0==cmp(self.buf,"exit()\r\n")):
				self.sendbuf = "connection will be closed"
                                self.clientsockfd.write("%s\n" %self.sendbuf)
				self.clientsockfd.write(end_flag)
                                self.clientsockfd.flush()
				#self.clientsock.shutdown(socket.SHUT_RDWR)
				self.clientsockClose(self.clientsock)
				self.clientsockfd.close()
				 # the connection is already close
                                self.logger.info("Monitoring connection closed by monitor %s." %self.clientaddr[0])
                                print "Monitoring connection closed by monitor %s." %self.clientaddr[0]
				return
			else:
				self.sendbuf = "unrecognized command!!"
				self.clientsockfd.write("%s\n" %self.sendbuf)
				self.clientsockfd.write(end_flag)
				self.clientsockfd.flush()
		self.clientsockfd.close() #change
##
# This Thread class is used for Listening 
#	data-recv port (this port is used by DataSources unit)
#
##
class Thread_datarecvport_listen(threading.Thread):
	def __init__(self,port,logger):
		threading.Thread.__init__(self)
		self.port = port
		self.host = ""
		self.logger = logger
		self.handle_unit = ''
		self.parent = ''

		# variable for monitoring
		self.server_starttime = time.localtime()
		self.currentday = time.strftime('%Y%j',time.localtime())
                self.total_count = 0
                self.day_total_count =0
                self.misunderstand_count = 0
                self.day_misunderstand_count = 0
		self.connection_timebuckets = [] 
	def addTotalCount(self):
		self.total_count = self.total_count + 1
	def getTotalCount(self):
		return self.total_count
	def addDayTotalCount(self):
		self.tmpday = time.strftime('%Y%j',time.localtime())
		if( self.tmpday > self.currentday ):
			self.currentday = self.tmpday
			self.day_total_count = 0
			self.day_misunderstand_count = 0
		self.day_total_count = self.day_total_count + 1
	def getDayTotalCount(self):
		return self.day_total_count
	def addMisunderstandCount(self):
		self.misunderstand_count = self.misunderstand_count + 1
	def getMisunderstandCount(self):
		return self.misunderstand_count
	def addDayMisunderstandCount(self):
		self.tmpday = time.strftime('%Y%j',time.localtime())
                if( self.tmpday > self.currentday ):
                        self.currentday = self.tmpday
                        self.day_total_count = 0
                        self.day_misunderstand_count = 0
		self.day_misunderstand_count = self.day_misunderstand_count + 1
	def getDayMisunderstandCount(self):
		return self.day_misunderstand_count
	def getServerStarttime(self):
		return self.server_starttime
	def getConnectionTimebuckets(self):
		return self.connection_timebuckets

	###########################################
	def setConnectionCloseTime(self):
		for timebucket in self.connection_timebuckets:
			if(0==timebucket[1]):
				timebucket[1] = time.localtime()
				self.logger.info("setConnectionCloseTime success,%s" %time.strftime('%Y%m%d %H%M%S',timebucket[1]))
				return
		self.logger.warning("setConnectionCloseTime fail, cannot find timebucket[1]==0")
		return
	def setPeerThreadInfo(self,peer_thread):
                self.sendlisten_thread = peer_thread
	def run(self):
		global recv_max_conn,DS_pattern,DS_unit
		try:
			self.sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
			self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR,1)
			self.sock.bind((self.host,self.port))
			self.sock.listen(recv_max_conn)	# at most 1 concurrent connection
			self.logger.info("recv_listening thread start listen to port %d ..." %self.port)
			print "recv_listening thread start listen to port %d ..." %self.port
		except Exception,ex:
			self.logger.error("Recv_listening thread cannot listen to the port %d!" %self.port)
			print "Recv_listening thread cannot listen to the port %d!" %self.port
			traceback.print_exc()
			self.logger.error(ex)
			sys.exit(1)
		
		#accept connections and start datarecvport_handle threads
		while 1:
			try:
				self.clientsock, self.clientaddr = self.sock.accept()
				#self.clientsock.settimeout(30)
				self.clientsock.settimeout(120)
				self.logger.info("Recv port accept a connection from client : %s" %self.clientaddr[0])
				print "Recv port accept a connection from client : %s" %self.clientaddr[0]
				#self.logger.info("Recv port accept a connection from client : %s" %self.clientaddr)
				#TODO:here should verified the client, whether it is JW or not, maybe by IP range
				if(False==isLegalDS(self.clientaddr[0],DS_pattern)):
					self.logger.info("Recv port connection %s will be closed becasue it seems not %s" %(self.clientaddr[0],DS_unit))
					print "Recv port connection %s will be closed becasue it seems not %s" %(self.clientaddr[0],DS_unit)
					try:
						#self.clientsock.shutdown(socket.SHUT_RDWR)
						self.clientsock.close()
					except:
						self.logger.critical("Recv port connection %s close fail!!" %self.clientaddr[0])
					continue
						
				self.handle_unit = Thread_datarecvport_handle(self.clientsock,self.clientaddr,self.logger)
				self.handle_unit.setDaemon(True)
				self.handle_unit.setParentThread(threading.currentThread())
				self.handle_unit.start()
				self.logger.info("Start a datarecv_handle thread.%s" %self.handle_unit)
				print "Start a datarecv_handle thread.%s" %self.handle_unit

				self.timebucket = [time.localtime(),0]
				self.connection_timebuckets.append(self.timebucket)
			except Exception,ex:
				self.logger.critical("Error happend at accept() - Thread_datarecvport_listen")
				print "Error happend at accept() - Thread_datarecvport_listen"
				traceback.print_exc()
				self.logger.critical(ex)
				#sys.exit(1)
##
# This Thread class is used for Listening 
#       data-send port (this port is used by JiaoWei)
#
##
class Thread_datasendport_listen(threading.Thread):
        def __init__(self,port,logger):
                threading.Thread.__init__(self)
                self.port = port
                self.host = ""
		self.parent = ''
                self.logger = logger
		self.dispatcher_thread = ''
	def setPeerThreadInfo(self,peer_thread):
		self.recvlisten_thread = peer_thread
	def registerAll(self,sendthread):
		if(self.dispatcher_thread):
			return self.dispatcher_thread.registerAll(sendthread)
		return False
	def unregisterAll(self,sendthread):
		if(self.dispatcher_thread):
			return self.dispatcher_thread.unregisterAll(sendthread)
		return False
        def run(self):
		global send_max_conn,G_max_count
                try:
                        self.sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
                        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR,1)
                        self.sock.bind((self.host,self.port))
                        self.sock.listen(send_max_conn)    # at most 50 concurrent connection
			self.logger.info("send_listening thread start listen to port %d ..." %self.port)
			print "send_listening thread start listen to port %d ..." %self.port
                except Exception,ex:
                        self.logger.error("Cannot listen to the port %d" %self.port)
			print "Cannot listen to the port %d" %self.port
			traceback.print_exc()
			self.logger.error(ex)
                        sys.exit(1)
		# start dispatcher thread
		self.dispatcher_thread = Thread_dispatcher(threading.currentThread(),logger)
		self.dispatcher_thread.setDaemon(True)
		#self.dispatcher_thread.setParentThread(threading.currentThread())
		self.dispatcher_thread.start()
		self.logger.info('Start dispatcher thread')
		print 'Start dispatcher thread'
		########################

                while 1:
                        try:
                                self.clientsock, self.clientaddr = self.sock.accept()
				if self.clientaddr[0] in G_connected_IPs:
					if (G_connected_IPs[self.clientaddr[0]] >= G_max_count):
						self.logger.warning("Send port warning: The IP %s is already have %d connection" %(self.clientaddr[0],G_max_count))
						print "Send port warning The IP %s is already have %d connection" %(self.clientaddr[0],G_max_count)
						#self.clientsock.shutdown(socket.SHUT_RDWR)
						self.clientsock.close()
						continue;
				print "%s:%s" %(self.clientaddr[0],self.clientaddr[1])
				self.clientsock.settimeout(60)	
                                self.logger.info("Send port accept a connection from client : %s:%s" %(self.clientaddr[0],self.clientaddr[1]))
				print "Send port accept a connection from client : %s:%s" %(self.clientaddr[0],self.clientaddr[1])
				self.handle_unit = Thread_datasendport_handle(self.clientsock,self.clientaddr,self.logger)
				self.handle_unit.setDaemon(True)
				self.handle_unit.setParentThread(threading.currentThread())
				self.handle_unit.start()
				self.logger.info("Start a datasend_handle thread.%s" %self.handle_unit)
				print "Start a datasend_handle thread.%s" %self.handle_unit
                        except Exception,ex:
                                self.logger.critical("Error happend at accept() - Thread_datasendport_listen")
				print "Error happend at accept() - Thread_datasendport_listen"
				traceback.print_exc()
				self.logger.error(ex)
                                #sys.exit(1)


##
# This Thread class is used for listening
#	monitoring port (this port is used by monitor-APP
#
##
class Thread_monitoringport_listen(threading.Thread):
	def __init__(self,port,logger):
		threading.Thread.__init__(self)
		self.port = port
		self.host = ""
		self.parent = ''
		self.logger = logger
		self.recvlisten_thread = recvlisten_thread
	def setParentThread(self,parent):
                self.parent = parent
	def run(self):
		global monitor_max_conn
		try:
			self.sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
			self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR,1)
			self.sock.bind((self.host,self.port))
			self.sock.listen(monitor_max_conn)    # at most 10 concurrent connection
			self.logger.info("Monitor thread start listen to port %d ..." %self.port)
			print "Monitor thread start listen to port %d ..." %self.port
                except Exception,ex:
                        self.logger.error("Monitor thread cannot listen to the port %d" %self.port)
                        print "Monitor thread cannot listen to the port %d" %self.port
                        traceback.print_exc()
			self.logger.error(ex)
                        sys.exit(1)
                while 1:
                        try:
                                self.clientsock, self.clientaddr = self.sock.accept()
                                self.clientsock.settimeout(300)
                                self.logger.info("Monitoring port accept a connection from client : %s" %self.clientaddr[0])
                                print "Monitoring port accept a connection from client : %s" %self.clientaddr[0]
                                self.handle_unit = Thread_monitoringport_handle(self.clientsock,self.clientaddr,self.logger)
                                self.handle_unit.setDaemon(True)
                                self.handle_unit.setParentThread(threading.currentThread())
				self.handle_unit.start()
				self.logger.info("Start a monitor_handle thread.%s" %self.handle_unit)
				print "Start a monitor_handle thread.%s" %self.handle_unit
                        except Exception,ex:
                                self.logger.critical("Error happend at accept() - Thread_monitoringport_listen")
                                print "Error happend at accept() - Thread_monitoringport_listen"
                                traceback.print_exc()
				self.logger.critical(ex)
                                #sys.exit(1)

#######################################################################################
#
#	Main this program starts here
#
#######################################################################################
#client_pwd = 'siatxdata\r\n'
#monitor_pwd = 'siatmonitor\r\n'
homedir=sys.path[0]

end_flag = 'pxb_dataguard\r\n'
os.system('mkdir '+homedir+'/Log')
logfile = homedir+'/Log/log.log'
logger = initHDlog(logfile)
#recv_port = 56547
#send_port = 5557
#monitoring_port = 5558

admin_list,all_list,client_pwd,monitor_pwd,recv_port,send_port,monitoring_port,recv_max_conn,send_max_conn,monitor_max_conn,G_max_count,DS_pattern,DS_unit,recv_function= loadConfig(homedir+"/config.ini")
Recv_module=__import__(recv_function)
client_pwd='%s\r\n' % client_pwd
monitor_pwd='%s\r\n' % monitor_pwd
for item in admin_list:
	addToList(G_AdminList,item)
for item in all_list:
	addToList(G_AppClientList_all,item)
print 'G_AdminList',G_AdminList
print 'G_AppClientList_all',G_AppClientList_all

recvlisten_thread = Thread_datarecvport_listen(recv_port,logger)
recvlisten_thread.setDaemon(True)

# start monitoring thread
monitoringlisten_thread = Thread_monitoringport_listen(monitoring_port,logger)
monitoringlisten_thread.setDaemon(True)

sendlisten_thread = Thread_datasendport_listen(send_port,logger)
sendlisten_thread.setDaemon(True)

sendlisten_thread.setPeerThreadInfo(recvlisten_thread)
sendlisten_thread.start()
logger.info('Start send port listen thread')
print 'Start send port listen thread'
recvlisten_thread.setPeerThreadInfo(sendlisten_thread)
recvlisten_thread.start()
logger.info('Start recv port listen thread')
print 'Start recv port listen thread'
monitoringlisten_thread.setParentThread(recvlisten_thread)
monitoringlisten_thread.start()
logger.info('Start monitoring port listen thread')
print 'Start monitoring port listen thread'


sendlisten_thread.join()
monitoringlisten_thread.join()
recvlisten_thread.join()
print "----------main end-------------"

