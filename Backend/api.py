try:
    from flask import app,Flask
    import pandas as pd
    from flask_restful import Resource, Api, reqparse
    import elasticsearch
    from elasticsearch import Elasticsearch
    import datetime
    import concurrent.futures
    import requests
    import json
except Exception as e:
    print("Modules Missing {}".format(e))


app = Flask(__name__)
api = Api(app)

#------------------------------------------------------------------------------------------------------------

NODE_NAME = 'companydatabase'
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

#------------------------------------------------------------------------------------------------------------


"""
{
"wildcard": {
    "title": {
        "value": "{}*".format(self.query)
    }
}
}

"""


class Controller(Resource):
    def __init__(self):
        self.query = parser.parse_args().get("query", None)
        self.baseQuery = {
                "size":10000,  
                "query": {
                    "bool": {
                    "should":{
                        "multi_match":{
                            "fields":["Address.edgengram","Interests.edgengram",
                            "Designation.edgengram","Interests.keywordstring"],
                            "query":"{}".format(self.query),
                            "type":"best_fields"
                        }
                    }
                    }
                },
                "highlight":{      
                    "pre_tags":[''],
                    "post_tags":[''],
                    "fields":[{"Address.edgengram":{}},{"Interests.edgengram":{}},
                    {"Designation.edgengram":{}},{"Interests.keywordstring":{}}]
                }  
                }

    def get(self):
        res = []
        if len(self.query) >3:
            response = es.search(index=NODE_NAME, body=self.baseQuery)
            df = pd.DataFrame([{**{"highlight":item.get("highlight")}} for item in response['hits']['hits']])
            if df.shape[0]>0:
                [res.append(x) for x in df.highlight.tolist() if x not in res] 
                res = res[:10]
        return {"result":[{"column":list(x.keys())[0], "key":list(x.values())[0][0]} for x in res]}


parser = reqparse.RequestParser()
parser.add_argument("query", type=str, required=True, help="query parameter is Required ")

api.add_resource(Controller, '/autocomplete')


if __name__ == '__main__':
    app.run(debug=True, port=4000)
