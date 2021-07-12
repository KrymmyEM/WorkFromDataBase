from collections import defaultdict
import configparser

import psycopg2
import psycopg2.extras

import json

config = configparser.ConfigParser()
config.read("Config.ini")

class WDB(): #Work from Database

    @classmethod
    def selectEx(cls, command, values = (None), first = False):

        def sql_to_data(cur):
            SQL = cur.fetchall()
            SQLDumps = json.dumps(SQL)
            SQLData = json.loads(SQLDumps)
            return SQLData

        with psycopg2.connect(dbname=config["Database"]["DB_NAME"], user=config["Database"]["DB_USER"],
                              password=config["Database"]["DB_PASS"], host=config["Database"]["DB_HOST"],
                              port="5432") as con:
            with con.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                wordArray = command.split()
                id = 0
                valueArray = list()
                valueKey = ""

                for word in wordArray:
                    if word == "SELECT" or word == "DISTINCT" or word == "ALL":
                        continue
                    elif word != "FROM":
                        valueArray.append(word)
                        id += 1
                    else:
                        break

                if len(valueArray) == 1:
                    valueKey = valueArray[0]
                    cur.execute(command, values)
                    Data = sql_to_data(cur)
                    List = WDB.getValueList(Data, (valueKey,), first)
                    return List
                else:
                    print(valueArray)
                    cur.execute(command, values)
                    Data = sql_to_data(cur)
                    WDB.getValueList(Data, tuple(valueArray), first)
                    return Data



    @classmethod
    def getValueList(cls,loaded, key = tuple(), first = False): #Method for get indenify scen in Json data

        id_dict = defaultdict(list)


        def get(loaded,key):
            for elements in loaded:
                for k, v in elements.items():
                    id_dict[k].append(v)


            return sorted(id_dict[key[0]])

        if first:
            id_dict = get(loaded, key)
            return id_dict[0]
        else:
            id_dict = get(loaded, key)
            return sorted(id_dict)
