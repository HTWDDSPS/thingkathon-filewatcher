from watchdog.observers.polling import PollingObserverVFS as Observer
from watchdog.events import FileSystemEventHandler
import time
import os
import asyncio
from asyncua import Client, Node, ua
import csv
import json

OPCUA_URL = "opc.tcp://192.168.21.141:53530/thingkathon/soba/"

class OPCUA:
    def __init__(self, opcua_uri):
        self.opcua = Client(opcua_uri)
    
    def set_file(self, filepath):
        self.file = filepath

    async def add_station(server,id,station_name):
        node = await server.nodes.objects.add_object(
            ua.NodeId.from_string(id),
            station_name,
            ua.NodeId.from_string("ns=3;i=1002")
        )
        return node
    
    async def add_wirkrichtung(parent, parent_name):
        node =  await parent.add_object(
            ua.NodeId.from_string('ns=2;s='+parent_name+'.Wirkrichtung'),
            parent_name,
            ua.NodeId.from_string("ns=3;i=1004")
        ) 
        return node
    
    async def add_messpunkt(parent, messpunkt_name):
        return await parent.add_object(
            ua.NodeId.from_string('ns=2;s='+str(messpunkt_name)),
            messpunkt_name,
            ua.NodeId.from_string("ns=3;i=1003")
        ) 
    
    async def upload(self):
        js = {}
        reader = csv.DictReader(open(self.file, newline=""), delimiter="\t")
    
        Punkte = []
        data = {}
        
        for row in reader:       
            for item in list(row.items()):
                name =  list(item)[0].split(" ")
                value =  list(item)[1]           
                name.append(value)
                Punkte.append(name)
                if name[1] not in data.keys():
                    data[name[1]] = ""

        for js in Punkte[15:-2]:    
            id = 'ns=2;s='+str(js[1])+'.Wirkrichtung.'+str(js[2])+'.'+str(js[0])
            
            try:
                node = self.opcua.get_node(ua.NodeId.from_string(id))

                await node.write_value(js[-1])
            except Exception as e:
                print(str(e), id)
        print(Punkte)

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