import socket
import os
from _thread import *
import time

def startnasaLogSimulator():
    ServerSideSocket = socket.socket()
    dataset_path = "/home/ammar/Downloads/Streaming_nasa_logs/sparkstreaming/nasalogs.csv"
    host = '127.0.0.1'
    port = 5551
    ThreadCount = 0
    try:
        ServerSideSocket.bind((host, port))
    except socket.error as e:
        print(str(e))
    print('Socket is listening..')
    ServerSideSocket.listen(5)

    def multi_threaded_client(connection):
        connection.send(str.encode('Server is working:')) 
        file_handle = open(dataset_path, 'r')
       
        with open(dataset_path, 'r') as file_handle:
            while True:
                try:
                    line = file_handle.readline()
                except: 
                    connection.close()
                    file_handle.close()   
                
                if line != "":  
                    try:
                        print(line, end='')
                        connection.sendall(line.encode())      
                    except socket.error:
                        connection.close()
                        file_handle.close()
                else:
                    file_handle.seek(0)
                time.sleep(1)

            connection.close()
    while True:
        Client, address = ServerSideSocket.accept()
        print('Connected to: ' + address[0] + ':' + str(address[1]))
        start_new_thread(multi_threaded_client, (Client, ))
        ThreadCount += 1
        print('Thread Number: ' + str(ThreadCount))
    ServerSideSocket.close()


if __name__ =="__main__":
    startnasaLogSimulator()