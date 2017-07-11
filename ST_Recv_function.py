//#!/usr/bin/python
#	here must delpoyment A function named recvAMessage(clientsock,logger_pointer,listen_thread)
# 		the return of recvAMessage are: (buf,action)
#			# actions:
#                       #       0 - close connection
#                       #       1 - distribute buf
#                       #       2 - reply client wiht buf
#                       #       3 - ignore
#
import socket
import logging
import struct

random_string = '1234567890'
login_acc = 'XJY'
login_pwd = 'XDATA'

#recv n bytes msg
def getNBytes(n,clientsock):
        temp1 = ''
        temp2 = ''
        result = ''
        while 1:
                if(n==len(temp1)):
                        result = temp1
                        return result
                else:
                        temp2 = clientsock.recv(n-len(temp1)) #
                        if(not temp2):
                                # the connection is alread close by peer  #???there's no data or error or empty string
                                return
                        temp1 = temp1 + temp2
                        continue
def getAPackage(clientsock):
	msg_tmp = ''
	msg_buf = ''
	msg_len = ''
	msg_data = ''
	skip_count = 0
	while 1:
		msg_tmp = clientsock.recv(1)
		if (not msg_tmp):
			return
		if(msg_tmp==struct.pack('b',126)):	# headflag 0x7E
			if skip_count!=0:
				print "skip %d bytes!!" %(skip_count)
			break
		skip_count = skip_count + 1;
	msg_len = getNBytes(2,clientsock)
	if not msg_len:
		# connection closed by peer
		return
	packet_length = struct.unpack('> H',msg_len)[0]
	data_length = packet_length - 1 - 2 - 1 # headerflag, packetlength, tailerflag
	msg_data = getNBytes(data_length,clientsock)
	if not msg_data:
		# connection closed by peer
		return
	msg_tmp = clientsock.recv(1)	# tailer flag
	if not msg_tmp:
		# connection closed by peer
		return
	if(msg_tmp!=struct.pack('b',35)):	# tailer flag not match
		print "tailer flag not match"
	msg_buf = msg_len + msg_data
	return msg_buf
def verifyLogin(bufData):
	return True
def formateSendMessage(msgType,msgData):
	msg_headflag = struct.pack('b',126)
	msg_header_Packetlength = 1+5+len(msgData)+1
	msg_header_Packetlength = struct.pack('> H',msg_header_Packetlength)
	msg_header_PacketID = msgType
	msg_header_GPSCenterID = 'B'
	msg_data = msgData
	msg_endflag = struct.pack('b',35)
	msg = msg_headflag + msg_header_Packetlength + msg_header_PacketID + msg_header_GPSCenterID + msg_data + msg_endflag
	return msg
# some protocol need to send a message to client first, here must implement initMessage() function, if no need to send a message return none
def initMessage():
	msg_type = struct.pack('> H',0)
	msg_data = random_string
	msg = formateSendMessage(msg_type,msg_data)
	print "initMessage len:%d, data_len:%d, data:%s" %(len(msg),len(msg_data),msg_data)
	return msg

#:recv a message, BTW when error happen, it has to setConnectionCloseTime()
#when cannot Jiexi a message, it has to addMisunderstandCount() and *** there are some error in this function need to be change later  
def recvAMessage(clientsock,logger,recvlisten_thread):
        misunderstand_flag = False
        msg_type=''
        msg_len=''
	msg_gps_centerID=''
        msg_value=''
        msg=''
	action = -1
	msg_buf = getAPackage(clientsock)
	if not msg_buf:
		print "msg_buf=''"
		action = 0
		return '', action
	msg_int_len = len(msg_buf)
	if msg_int_len < 5:
		action = 3
		print "msg_int_len<5"
		recvlisten_thread.addMisunderstandCount()
                recvlisten_thread.addDayMisunderstandCount()
		return '', action
	msg_len_org = struct.unpack('> H',msg_buf[:2])[0]
	if (msg_len_org!=(len(msg_buf)+2)):         # an error happend
		action = 3
		print "msg_len_org!=(len(msg_buf)+2))"
		print "msg_len_org =",msg_len_org
		print "len(msg_buf) =",len(msg_buf)
		recvlisten_thread.addMisunderstandCount()
                recvlisten_thread.addDayMisunderstandCount()
		return '',action
	msg_type = msg_buf[2:4]	
	msg_gps_centerID = msg_buf[4:5]
	msg_value = msg_buf[5:]
	#print "get a message"
	if (msg_type==struct.pack('> H',0)):		#0x0000
		logger.info("get LOGIN_RANDOM_SERIAL message.")
		#dimiss
		action = 3
		msg = ''
	elif (msg_type==struct.pack('> H',4097)):	#0x1001
		logger.info("get LOGIN_REQ message.")
		#TODO will valid login later
		msgType = struct.pack('> H',36865)
		msgData = struct.pack('> L',0)
		msg = formateSendMessage(msgType,msgData)
		action = 2
	elif (msg_type==struct.pack('> H',36865)):      #0x9001
		logger.info("get LOGIN_RSP message.")
		action = 3
		msg = ''
	elif (msg_type==struct.pack('> H',4098)):       #0x1002
		logger.info("get LOGOUT_REQ message.")
		#TODO will valid logout later
		msgType = struct.pack('> H',36866)
		msgData = struct.pack('> L',0)
		msg = formateSendMessage(msgType,msgData)
		action = 2
	elif (msg_type==struct.pack('> H',36866)):      #0x9002
		logger.info("get LOGOUT_RSP message.")
		msg = ''
		action = 3
	elif (msg_type==struct.pack('> H',4099)):       #0x1003
		logger.info("get LINKTEST_REQ message.")
		msgType = struct.pack('> H',36867)
		msgData = ''
		msg = formateSendMessage(msgType,msgData)
		action = 2
	elif (msg_type==struct.pack('> H',36867)):      #0x9003
		logger.info("get LINKTEST_RSP message.")
		msg = ''
		action = 3
	elif (msg_type==struct.pack('> H',4100)):       #0x1004
		logger.info("get APPLY_REQ message.")
		msg = ''
		action = 3
	elif (msg_type==struct.pack('> H',36868)):      #0x9004
		logger.info("get APPLY_RSP message.")
		msg = ''
		action = 3
	elif (msg_type==struct.pack('> H',1)):       	#0x0001
		#logger.info("get DELIVER message.")
		msg_len = struct.pack('> H',msg_int_len)
		#print "msg_int_len:",msg_int_len
		msg = msg_type + msg_len + msg_gps_centerID + msg_value
		action = 1
        return msg,action


















