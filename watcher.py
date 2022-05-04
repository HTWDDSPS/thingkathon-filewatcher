import imp
from watchdog.observers.polling import PollingObserverVFS as Observer
from watchdog.events import FileSystemEventHandler
import time
import os
import asyncio
from asyncua import Client, Node, ua
import csv
import json
from domain.parser import parse
OPCUA_URL = "opc.tcp://192.168.21.141:4840/freeopcua/server/"

class OPCUA:
    def __init__(self, opcua_uri):
        self.opcua = Client(opcua_uri)
    
    def set_file(self, filepath):
        self.file = filepath



    def parse(self):
        with open(self.file, newline="") as csvfile:
            reader = csv.DictReader(csvfile, delimiter="\t")
            
            Messpunkte = []
            Punkte = []
            MesspunkteDict = {}
            for row in reader:       
                for item in list(row.items()):
                    name =  list(item)[0].split(" ")
                    value =  list(item)[1]           
                    name.append(value)
                    Punkte.append(name)
                    if name[1] not in MesspunkteDict.keys():
                           MesspunkteDict[name[1]] = ""
          
        
            return Punkte

    async def upload(self):
        
        url = 'opc.tcp://192.168.21.141:4840/freeopcua/server/'
    # url = 'opc.tcp://commsvr.com:51234/UA/CAS_UA_Server'
        async with Client(url=url) as client:
            # Client has a few methods to get proxy to UA nodes that should always be in address space such as Root or Objects
            # Node objects have methods to read and write node attributes as well as browse or populate address space
            

            uri = 'http://examples.freeopcua.github.io'
            idx = await client.get_namespace_index(uri)

            jso = (self.parse())
            # get a specific node knowing its node id
            # var = client.get_node(ua.NodeId(1002, 2))
            # var = client.get_node("ns=3;i=2002")
            w = jso[15:-2]
            i = 0
            for js in w:    
                
                id = 'ns=2;s='+str(js[1])+'.Wirkrichtung.'+str(js[2])+'.'+str(js[0])
                #print(id)
                try:
                    node = client.get_node(ua.NodeId.from_string(id))
                    await node.write_value(float(js[-1].replace(',', '.')))
                    print("written: {0} with value: {1}".format(id, js[-1]))
                except Exception as e:
                    pass
            print("written")



class EventHandler(FileSystemEventHandler):
    def __init__(self, opcua_uri):
        self.opcua = OPCUA(opcua_uri)

    def on_created(self, event):
        if event.is_directory:
            return None

        self.opcua.set_file(event.src_path)

        loop = asyncio.new_event_loop()
        a1 = loop.create_task(self.opcua.upload())
        loop.run_until_complete(a1)
        loop.stop()
        loop.close()


if __name__ == "__main__":
    observer = Observer(os.stat, os.listdir, 1)
    eventhandler = EventHandler(OPCUA_URL)
    observer.schedule(eventhandler, path="/mnt/csv_files", recursive=True)
    observer.start()

    try:
        while True:
            time.sleep(1)
    finally:
        observer.stop()
        observer.join()