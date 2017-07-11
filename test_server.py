//#!/usr/bin/python
import socket
import struct
###################################################################
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

def initMessage():
	random_string = '1234567890'
        msg_type = struct.pack('> H',0)
        msg_data = random_string
        msg = formateSendMessage(msg_type,msg_data)
        print "initMessage len:%d, data_len:%d, data:%s" %(len(msg),len(msg_data),msg_data)
        return msg

###################################################################
host = ""
port = 20000

sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
sock.bind((host,port))
sock.listen(10)

clientsock, clientaddr = sock.accept()
print "Recv a connection from "
msg_buf = initMessage()
clientsock.sendall(msg_buf)
count = 0
while 1:
	buf = clientsock.recv(10)
	if( not buf ):
		print "connection closed"
		clientsock.close()
		break
	print buf
	count = count + 1
	if(count%100000==1):
		print 'recv %d messages' %count
