#!/usr/bin/env python
import socket, select, os, sys, random, json, copy, datetime, pprint

class Node:
    def __init__(self):
        self.sock          = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.host          = socket.gethostname()
        self.port          = 15001
        self.nodeID        = random.randint(1, (2 ** 16) - 2)
        self.address       = (self.host, self.port)
        self.bootStrapHost = 'silicon.cs.umanitoba.ca'
        self.bootStrapPort = 15000
        self.bootStrapAddr = (self.bootStrapHost, self.bootStrapPort)
        self.successorName = ""
        self.successorPort = 0
        self.successorID   = 0

    #------------------------------------------------------------------------------------------------#
    def start(self):

        try:
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.sock.bind(self.address)
            print("Initalizaed on: %s, and port: %d with ID: %s") %(self.host, self.port, self.nodeID)

        except:
            print("Socket creation error")
            sys.exit(1)

        #Setting my(nodes) port, ID, and hostname
        jsonFile = open("message.json", "r") # Open the JSON file for reading
        data     = json.load(jsonFile) # Read the JSON into the buffer
        jsonFile.close() # Close the JSON file

        ## Working with buffered content
        data["ID"]       = self.nodeID
        data["port"]     = self.port
        data["hostname"] = self.host 

        ## Save our changes to JSON file
        jsonFile = open("message.json", "w+")
        jsonFile.write(json.dumps(data))
        jsonFile.close()

        #-----------------------------------------------------#

        jsonFile = open("thePred.json", "r") # Open the JSON file for reading
        data     = json.load(jsonFile) # Read the JSON into the buffer
        jsonFile.close() # Close the JSON file

        ## Working with buffered content
        data["ID"]       = 0
        data["port"]     = 15000
        data["hostname"] = "silicon.cs.umanitoba.ca" 

        ## Save our changes to JSON file
        jsonFile = open("thePred.json", "w+")
        jsonFile.write(json.dumps(data))
        jsonFile.close()
    
        self.joinRing()
        self.waiting()
    #------------------------------------------------------------------------------------------------#
    def waiting(self):

        print("Now that i've joined the ring, ill wait for a message")
        self.sock.settimeout(30)
        try:           
            while True:
                socketFd = self.sock.fileno()

                (readFd, writeFd, errorFd) = select.select([socketFd, sys.stdin], [], [], 0.0)

                for item in readFd:
                    if(item == sys.stdin):
                        string = sys.stdin.readline()
                        if(string == "\n"):
                            print("Ending, goodbye...")
                            self.sock.close()
                            exit(1)
                        else:
                            try:
                                number = int(string)
                            except:
                                print("error, you did not enter a number for the query")
                            else:
                                if(number > self.nodeID):
                                    self.startQuery(number)
                                else:
                                    print("%s is not a valid request number, I will generate a random number for you.") % (number)
                                    number = random.randint(self.nodeID,2 ** 16 - 2)
                                    print("will use %s instead") % (number)

                                    self.startQuery(number)
                    elif(item == socketFd):

                        data, address = self.sock.recvfrom(4096)

                        try:
                            #convert string back to json object 
                            dataObj = json.loads(data)

                        except ValueError:
                            print("%s is not in valid json format...") % (data)
                            self.sock.close()
                            exit(1)

                        else:
                            if(dataObj["cmd"] == "setPred"):
                                print("\n\n\nhandling 'setPred' command from %s") % (socket.gethostbyaddr(address[0])[0])
                                print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                                self.stabilize()

                                jsonFile = open("thePred.json", "r") # Open the JSON file for reading
                                data     = json.load(jsonFile) # Read the JSON into the buffer
                                jsonFile.close() # Close the JSON file

                                if(self.nodeID > int(dataObj["ID"])):

                                    print("Changing my pred from %s, on port %s, with ID %s to.... %s, on port %s, with ID %s") % (data["hostname"], data["port"], data["ID"], dataObj["hostname"], dataObj["port"], dataObj["ID"])
                                    data["hostname"] = dataObj["hostname"]
                                    data["port"]     = dataObj["port"]
                                    data["ID"]       = dataObj["ID"]

                                    # Save our changes to JSON file
                                    jsonFile = open("thePred.json", "w+")
                                    jsonFile.write(json.dumps(data))
                                    jsonFile.close()
                                    
                                else:
                                    print("Error, I will not accept a setPred command with ID: %s greater than mine: %s") %(dataObj["ID"], self.nodeID)

                            elif(dataObj["cmd"] == "pred?"):
                                print("\n\n\nhandling 'pred?' command from % s") % (socket.gethostbyaddr(address[0])[0])
                                print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                                self.stabilize()

                                jsonFile = open("thePred.json", "r") # Open the JSON file for reading
                                data     = json.load(jsonFile) # Read the JSON into the buffer
                                jsonFile.close() # Close the JSON file

                                response            = {}
                                response["me"]      = {"hostname": self.host, "port": self.port, "ID": self.nodeID}
                                response["cmd"]     = "myPred"
                                response["thePred"] = {"hostname": data["hostname"], "ID": data["ID"], "port": data["port"]} 

                                print("sending back: %s, %s") %(response["thePred"]["hostname"], response["thePred"]["ID"])
                                self.sock.sendto(json.dumps(response), address)

                            elif(dataObj["cmd"] == "find"):
                                print("\n\n\nhandling 'find' command from % s") % (socket.gethostbyaddr(address[0])[0])
                                print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                                self.handleQuery(dataObj)

                            elif(dataObj["cmd"] == "owner"):
                                print("Invalid query request: 'owner', im not supposed to recive this request ever")

        except socket.timeout:
            print("Opps, the socket timed out...restart the Node")
            self.sock.close()
            exit(1)

    #------------------------------------------------------------------------------------------------#
    def startQuery(self,number):

        assert type(number) is int, "not a int: %r" % number

        self.stabilize()

        self.setCmd("find")

        jsonFile = open("message.json", "r") # Open the JSON file for reading
        data     = json.load(jsonFile) # Read the JSON into the buffer
        jsonFile.close()

        data["hops"]  = 0
        data["query"] = number

        #converts to a string
        toSend = json.dumps(data)

        self.sock.settimeout(.9)
        try:
            data, server = self.askSuccessor(toSend, (self.successorName, self.successorPort), "nothing")
            data         = json.loads(data)

        except ValueError:
            print("%s is not in valid json format... recived from my successor during query") % (data)

        except socket.timeout:
            print("Error, didnt get a fast enough response for my query, something went wrong with my successor")

        else:
            ownerName = data["hostname"]
            numHops   = data["hops"]
            ownerID   = data["ID"]

            print("\n" +datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

            print("Number of hops needed for query--%s") % numHops
            print("Owner name of the query----------%s") % ownerName
            print("The query itself query-----------%s") % number
            print("Owner ID of the query------------%s") % ownerID
        
        finally:
            pass
    #------------------------------------------------------------------------------------------------#
    def handleQuery(self, data):

        self.stabilize()

        #if i can handle the query
        if(int(data["query"]) <= self.nodeID):

            data["cmd"]  = "owner"
            # data["hops"] = int(data["hops"]) + 1

            hostToSend = data["hostname"]
            portToSend = data["port"]

            data["hostname"] = self.host
            data["port"]     = self.port
            data["ID"]       = self.nodeID

            #converts to a string
            toSend = json.dumps(data)

            print("I can handle the query, this is what ill send back %s") %(toSend)

            self.sock.sendto(toSend, (hostToSend, portToSend))

        #else pass the request up
        else:
            print("I cant handle the query, so ill pass it up to my successor to handle...")
            data["hops"] = int(data["hops"]) + 1

            #converts to a string
            toSend = json.dumps(data)

            self.sock.sendto(toSend, (self.successorName, self.successorPort))
    #------------------------------------------------------------------------------------------------#
    def joinRing(self):

        copy = None

        self.setCmd("pred?")

        jsonFile = open("message.json", "r") # Open the JSON file for reading
        data = json.load(jsonFile) # Read the JSON into the buffer

        #converts to a string
        toSend = json.dumps(data)

        jsonFile.close()
        
        print("Starting my search at %s for my spot in the ring") %(self.bootStrapHost)

        data, server = self.askSuccessor(toSend, self.bootStrapAddr, "nothing")

        #convert string back to json object 
        data = json.loads(data)

        if (data["cmd"] == "myPred"):

            while("thePred" in data and int(data["thePred"]["ID"]) > int(self.nodeID)):

                print("\n\n\n%s's ID: %s, on port: %s is greater than my ID: %s") %(data["thePred"]["hostname"], data["thePred"]["ID"], data["thePred"]["port"] , self.nodeID)

                copy = server

                self.successorName = data["me"]["hostname"]
                self.successorPort = data["me"]["port"]
                self.successorID   = data["me"]["ID"] 

                data, server = self.askSuccessor(toSend, (data["thePred"]["hostname"], data["thePred"]["port"]), server)

                #convert string back to json object 
                data = json.loads(data)
                
            if("thePred" in data and data["thePred"]["hostname"] != "silicon.cs.umanitoba.ca" and data["thePred"]["ID"] > self.nodeID):
                self.successorName = data["thePred"]["hostname"]
                self.successorPort = data["thePred"]["port"]
                self.successorID   = data["thePred"]["ID"]

            elif("thePred" in data and data["me"]["ID"] > self.nodeID):
                self.successorName = data["me"]["hostname"]
                self.successorPort = data["me"]["port"]
                self.successorID   = data["me"]["ID"] 

            #the case if I found the old me from a previous join, replace it with the new me
            if(self.successorName != self.host and self.successorPort != self.port):
                print("This Node will be my successor to start : %s, on port: %s, with ID: %s") %(self.successorName, self.successorPort, self.successorID)

                #now that we have found the right place to join the ring, set my successors predicessor to me
                self.setCmd("setPred")

                jsonFile = open("message.json", "r") # Open the JSON file for reading
                data     = json.load(jsonFile) # Read the JSON into the buffer

                #converts to a string
                toSend = json.dumps(data)

                jsonFile.close()

                #the actuall setting of my successors predicessor
                self.sock.sendto(toSend,(self.successorName, int(self.successorPort)))
            else:
                print("This Node will be my successor to start : %s, on port: %s, with ID: %s") %(self.successorName, self.successorPort, self.successorID)

                #now that we have found the right place to join the ring, set my successors predicessor to me
                self.setCmd("setPred")

                jsonFile = open("message.json", "r") # Open the JSON file for reading
                data     = json.load(jsonFile) # Read the JSON into the buffer

                #converts to a string
                toSend = json.dumps(data)

                jsonFile.close()

                #the actuall setting of my successors predicessor
                self.sock.sendto(toSend,(self.successorName, int(self.successorPort)))
        else:
            print("Error joining the ring...")
            exit(1)

    #------------------------------------------------------------------------------------------------#
    def stabilize(self):

        print("stabilizing...")
        self.setCmd("pred?")

        jsonFile = open("message.json", "r") # Open the JSON file for reading
        data     = json.load(jsonFile) # Read the JSON into the buffer

        #converts to a string
        toSend = json.dumps(data)

        jsonFile.close()

        #first, checking if my successor still responds
        self.sock.settimeout(1.5)
        try:
            data, server = self.askSuccessor(toSend, (self.successorName, self.successorPort), "nothing")

            #convert string back to json object 
            data = json.loads(data)

        except ValueError:
            print("%s is not in valid json format... recived from my successor, ill just rejoin the ring") % (data)
            self.joinRing()

        except socket.timeout:
            print("could not contact my successor during stabilization, so i will rejoin the ring")
            self.joinRing()        

        else:
            if("thePred" in data):

                #if my successors pred is me.... do nothing
                if(data["thePred"]["hostname"] == self.host and data["thePred"]["port"] == self.port):
                    print("All is good, nothing has changed")
                    pass

                #else
                #could be one of two things(ok well maybe 3)
                #new pred could have less id than mine, greater id than mine, or same(very unlikely if random)
                else:
                    if(int(data["thePred"]["ID"]) > int(self.nodeID)):

                        self.successorName = socket.gethostbyaddr(server[0])[0]
                        self.successorPort = server[1]

                        self.setCmd("setPred")
                        jsonFile = open("message.json", "r") # Open the JSON file for reading
                        data     = json.load(jsonFile) # Read the JSON into the buffer

                        #converts to a string
                        toSend = json.dumps(data)

                        jsonFile.close()

                        print("changing my succesor to: %s on port: %s") % (self.successorName, self.successorPort)
                        self.sock.sendto(toSend,(self.successorName, self.successorPort))

                    elif(int(data["thePred"]["ID"]) < int(self.nodeID)):

                        self.setCmd("setPred")
                        jsonFile = open("message.json", "r") # Open the JSON file for reading
                        data     = json.load(jsonFile) # Read the JSON into the buffer

                        #converts to a string
                        toSend = json.dumps(data)

                        jsonFile.close()

                        self.sock.sendto(toSend,(self.successorName, int(self.successorPort)))
            else:
                self.joinRing()
    #------------------------------------------------------------------------------------------------#
    def askSuccessor(self, data, address, error): #ask node for its successor

        assert type(data) is str, "data is not a str: %r" % data
        assert type(address) is tuple, "adress is not a tuple: %r" % data

        self.sock.settimeout(2)
        try:
            self.sock.sendto(data, (address[0], int(address[1])))

            data, server = self.sock.recvfrom(4096)

            return data, server
            
        except socket.timeout:
            
            print("error, did not get a response message from: %s,soo Ring is broken :(") %(address[0])
            return data, error

    #------------------------------------------------------------------------------------------------#
    def setCmd(self, string):
        
        assert type(string) is str, "string is not a str: %r" % string
        assert string in ["pred?", "myPred", "setPred", "find", "owner"], "Not a valid value for cmd: %r.." % string

        jsonFile = open("message.json", "r") # Open the JSON file for reading
        data = json.load(jsonFile) # Read the JSON into the buffer
        jsonFile.close() # Close the JSON file

        ## Working with buffered content
        data["cmd"] = string

        ## Save our changes to JSON file
        jsonFile = open("message.json", "w+")
        jsonFile.write(json.dumps(data))
        jsonFile.close()
#-------------------------------------end Node class-------------------------------------------------#
#----------------------------------------------------------------------------------------------------#

def main(argv):
    print("Starting")
    node = Node()
    node.start()
    pass

if __name__ == "__main__":
    main(sys.argv)