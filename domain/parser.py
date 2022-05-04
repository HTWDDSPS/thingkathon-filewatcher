import csv
import json
# letzte Punkt Fixture

def parse(filepath):
    with open(filepath, newline="") as csvfile:
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
        

    with open("dump.json","w") as file:
        json.dump(Punkte,file)
    
    return Punkte



'''          
f = open("./dump.txt","w")
f.write(str(Punkte))
f.close()
'''