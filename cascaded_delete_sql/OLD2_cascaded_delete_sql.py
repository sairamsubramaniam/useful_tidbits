
from collections import defaultdict
import csv
import copy
import logging
import queue

import sqlalchemy as sa

from graphdfs_algo import graphdfs_algo as ga


logging.basicConfig(filename="sql_delete.log", level=logging.DEBUG)


INPUT = {
    "db": "wcore_dev_db",
    "table": "organization",
    "colname": "id",
    "colvalues": ["012e7653-b36c-4e46-bfc4-773eeea89bcd"]
}


ENGINE = sa.create_engine("mysql://wcore_db_user:pheic8thai2Nae6@206.189.137.69:3306/wcore_dev_db?charset=utf8")
DB = ENGINE.connect()


SELECT_QUERY_TEMPLATE = "select * from {db}.{table} where {colname} in :colvalues"
DELETE_QUERY_TEMPLATE = "delete from {db}.{table} where {colname} in :colvalues"


GET_RELATIONS_QUERY = """
select 
    TABLE_SCHEMA , 
    TABLE_NAME , 
    COLUMN_NAME ,
    REFERENCED_TABLE_SCHEMA , 
    REFERENCED_TABLE_NAME , 
    REFERENCED_COLUMN_NAME , 
    POSITION_IN_UNIQUE_CONSTRAINT 
from information_schema.KEY_COLUMN_USAGE kcu 
where CONSTRAINT_SCHEMA = '{db}'
and CONSTRAINT_NAME != 'PRIMARY'
and REFERENCED_TABLE_NAME is not null ;
""".format(db=INPUT["db"])



# ------------------ GRAPH BREADTH FIRST SEARCH ALGORITHM ---------------------


def get_adjacent_dict(E, vertex, colval):
    adjacent_vertices = E.get(vertex, [])
    return adjacent_vertices




def graphBFS(node_dict, G, start, get_adj_func, goal=None):
    path = []
    V, E = G["V"], G["E"]
    stack = queue.Queue()
    visited = set()
    stack.put(start)
    
    while not stack.empty():
        current = stack.get()
        path.append(current)
        visited.add(current)

        # Get colvalue after querying
        node_dict[current].run_select_query()
        
        if current == goal:
            return path
        
        adjacent_vertices = get_adj_func(E=E, vertex=current)
        for v in adjacent_vertices:
            if v not in visited:
                stack.put(v)
    
    if not goal:
        return path

    return []




# ------------------ CREATE GRAPH DATA STRUCTURE ---------------------


def create_identifier(db, table, colname, colvalues=None):
    return str(db) + "___" + str(table) + "___" + str(colname)


class Node(object):

    def __init__(self, db, table, colname, colvalues=[], 
                       parents=[], children=[]):
        self.db = db
        self.table = table
        self.colname = colname
        self.colvalues = colvalues
        self._level = None


    def __eq__(self, oth):
        if isinstance(oth, self.__class__):
            return oth.__dict__ == self.__dict__
        return False


    def __hash__(self):
        uniq_str = repr(self)
        return hash(uniq_str)

    
    @property
    def level(self):
        return self._level


    @level.getter
    def level(self):
        return self._level


    @level.setter
    def level(self, lev=None):
        self._level = lev


    def get_select_query(self):
        q = SELECT_QUERY_TEMPLATE.format(db=self.db,
                                         table=self.table,
                                         colname=self.colname)
        q_params = {"colvalues": self.colvalues}
        return q, q_params


    def run_select_query(self):
        if not self.colvalues:
            raise Exception("colvalues missing")

        q, params = self.get_select_query()
        resprox = DB.execute(sa.text(q), **params)
        results = []
        for k in resprox.fetchall():
            results.append(dict(k))
        return results

    
    def get_delete_query(self):
        q = DELETE_QUERY_TEMPLATE.format(db=self.db,
                                         table=self.table,
                                         colname=self.colname)
        q_params = {"colvalues": self.colvalues}
        return q, q_params

    def __repr__(self):
        s = create_identifier(self.db, self.table, self.colname)
        s += ( "___" + str(self.colvalues) )
        return s

    



def extract_child_parent_dict(row_dict):
    child_dict = {
        "db": row_dict["TABLE_SCHEMA"],
        "table": row_dict["TABLE_NAME"],
        "colname": row_dict["COLUMN_NAME"]
    }
    parent_dict = {
        "db": row_dict["REFERENCED_TABLE_SCHEMA"],
        "table": row_dict["REFERENCED_TABLE_NAME"],
        "colname": row_dict["REFERENCED_COLUMN_NAME"]
    }
    return child_dict, parent_dict


def input_without_value(row_dict):
    rd = copy.deepcopy(row_dict)
    val = rd.pop("colvalues")
    return rd


def create_col_graph_without_levels():
    resprox = DB.execute(GET_RELATIONS_QUERY)
    parents_dict = defaultdict(list)
    children_dict = defaultdict(list)
    node_dict = {}
    cntr = 0
    for row in resprox.fetchall():
        r = dict(row)

        child_dict, parent_dict = extract_child_parent_dict(row_dict=r)
        node_c = Node(**child_dict)
        node_p = Node(**parent_dict)

        parents_dict[repr(node_c)].append(node_p)
        children_dict[repr(node_p)].append(node_c)

        if repr(node_c) not in node_dict:
            node_dict[repr(node_c)] = node_c
        if repr(node_p) not in node_dict:
            node_dict[repr(node_p)] = node_p
        
    return node_dict, parents_dict, children_dict


def print_select_statements(node_dict, child_dict):

    node_id = create_identifier(**INPUT) + "___[]"

    V = set( node_dict.keys() )
    E = { k: conv_to_repr(v) for k, v in child_dict.items() }
    G = {"V":V, "E":E}

    nodes = ga.graphBFS(G=G, start=node_id, get_adj_func=ga.get_adjacent_dict )
    queries = [  [node_dict[k].get_select_query()] for k in nodes  ]

    with open("tttttttt.csv", "w") as otfl:
        wt = csv.writer(otfl, delimiter="\t")
        wt.writerow(["query"])
        wt.writerows(queries)


def conv_to_repr(lst):
    return [ repr(k) for k in lst ]



node_dict, parents_dict, children_dict = create_col_graph_without_levels()

root_str = create_identifier(**INPUT) + "___[]"

node_dict[root_str].colvalues = INPUT["colvalues"]
root_result = node_dict[root_str].run_select_query()

print_select_statements(node_dict, children_dict)



# print("******************")
# print( dict(parents_dict) )
# print("******************")
# print( dict(children_dict) )
# print("******************")
# print( dict(node_dict) )
# 
# 
# def create_col_graph_without_levels():
#     resprox = DB.execute(GET_RELATIONS_QUERY)
#     parents_dict = defaultdict(list)
#     children_dict = defaultdict(list)
#     node_dict = {}
#     cntr = 0
#     for row in resprox.fetchall():
#         r = dict(row)
# 
#         child_dict, parent_dict = extract_child_parent_dict(row_dict=r)
#         node_c = Node(**child_dict)
#         node_p = Node(**parent_dict)
# 
#         parents_dict[repr(node_c)].append(node_p)
#         children_dict[repr(node_p)].append(node_c)
# 
#         if repr(node_c) not in node_dict:
#             node_dict[repr(node_c)] = node_c
#         if repr(node_p) not in node_dict:
#             node_dict[repr(node_p)] = node_p
#         
#     return node_dict, parents_dict, children_dict



