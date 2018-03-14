import simplejson as json
class config_json:
    data = None
    def __init__(self):
        self.data = json.load(open('config.json'))

    def read(self):
        return(self.data)

    def set(self,key,value):
    	tmp = self.data
    	self.data[key] = value

    	with open("config.json", "w") as jsonFile:
    		json.dump(self.data, jsonFile)

    def setArray(self,value):
    	tmp = self.data
    	self.data['config_subreddits'] = value

    	with open("config.json", "w") as jsonFile:
    		json.dump(self.data, jsonFile)

