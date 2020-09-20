
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


def get_adjacent_dict(E, vertex, colname, colval, node_dict):
    adjacent_vertices = E.get(vertex, [])
    for v in adjacent_vertices:
        tbl_cls = node_dict[v]
        col_detail_dict = tbl_cls.parent_keys[vertex]

        for k, v in col_detail_dict.items():
            if k == colname:
                results = tbl_cls.run_parent_link_query( self_key=v, colval=colval )
                for row in results:
                    for ky in self.keys:
                        new_self_val = row.get(ky)
                        if new_self_val:
                            tbl_cls.keys[ky] = new_self_val
                            break


    return adjacent_vertices




def graphBFS(node_dict, G, start, get_adj_func, goal=None):
    path = []
    queries = []
    V, E = G["V"], G["E"]
    stack = queue.Queue()
    visited = set()
    stack.put(start)
    
    while not stack.empty():
        current = stack.get()
        path.append(current)
        queries.append( "" )
        visited.add(current)

        if current == goal:
            return [path, queries]
        
        adjacent_vertices = get_adj_func(E=E, vertex=current)
        for v in adjacent_vertices:
            if v not in visited:
                stack.put(v)
    
    if not goal:
        return [path, queries]

    return [[], []]




# ------------------ CREATE GRAPH DATA STRUCTURE ---------------------


def create_identifier(db, table):
    return str(db) + "___" + str(table)


class TableClass(object):

    def __init__(self, db, table):
        self.db = db
        self.table = table
        self.parent_keys = defaultdict(dict)
        self.keys = defaultdict(list)
        self.child_keys = []
        self.colvalues = []


    def __eq__(self, oth):
        if isinstance(oth, self.__class__):
            return oth.__dict__ == self.__dict__
        return False


    def __hash__(self):
        uniq_str = repr(self)
        return hash(uniq_str)


    def add_key(self, key, val=[]):
        self.keys[key].append(val)


    def add_child_key(self, table_id, key, val=None):
        self.child_keys.append( {table_id: {key:val}} )


    def add_parent_key(self, table_id, parent_key, self_key, val=None):
        self.parent_keys[table_id][parent_key] = self_key
        self.parent_keys[table_id][parent_key+"_value_"] = val

    
    def get_select_query(self, parent_id=None, parent_col=None):
        queries = []

        if self.keys:
            for k, v in self.keys:
                q = SELECT_QUERY_TEMPLATE.format(db=self.db,
                                                 table=self.table,
                                                 colname=k)
                q_params = {"colvalues": v}
                queries.append( (q, q_params) )

        if self.parent_keys:
            print(self.parent_keys)
            qr = self.get_parent_select_query(parent_id=parent_id, parent_col=parent_col)
            queries.append(qr)

        return queries


    def run_parent_link_query(self, self_key, colval):
        q = SELECT_QUERY_TEMPLATE.format(db=self.db,
                                         table=self.table,
                                         colname=self_key
                                        )
        q_params = {"colvalues": colval}

        resprox = DB.execute(sa.text(q), **q_params)

        return [dict(k) for k in resprox.fetchall()]


    def get_parent_select_query(self, parent_id, parent_col):
        self_col = self.parent_keys[parent_id][parent_col]
        self_colval = self.parent_keys[parent_id][parent_col+"_value_"]
        q = SELECT_QUERY_TEMPLATE.format(
                db=self.db,
                table=self.table,
                colname=self_col)
        q_params = { "colvalues": self_colval }
        return q, q_params


    def run_select_query(self, mode):
        if not self.colvalues:
            raise Exception("colvalues missing")

        if mode == "own":
            q, params = self.get_select_query()
        elif mode == "as_parent":
            q, params = self.getparent_select_query()
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
        s = create_identifier(self.db, self.table)
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



def create_table_classes():

    resprox = DB.execute(GET_RELATIONS_QUERY)
    parents_dict = defaultdict(list)
    children_dict = defaultdict(list)
    node_dict = {}

    for row in resprox.fetchall():
        r = dict(row)

        child_dict, parent_dict = extract_child_parent_dict(row_dict=r)
        node_c = TableClass(db=child_dict["db"], table=child_dict["table"])
        node_p = TableClass(db=parent_dict["db"], table=parent_dict["table"])

        node_p.add_key( key=parent_dict["colname"] )
        # node_p.add_child_key( table_id=repr(node_c), key=child_dict["colname"] )

        node_c.add_parent_key( table_id=repr(node_p), 
                               parent_key=parent_dict["colname"],
                               self_key=child_dict["colname"]
                             )

        parents_dict[repr(node_c)].append(node_p)
        children_dict[repr(node_p)].append(node_c)

        if repr(node_c) not in node_dict:
            node_dict[repr(node_c)] = node_c
        if repr(node_p) not in node_dict:
            node_dict[repr(node_p)] = node_p
        
    return node_dict, parents_dict, children_dict


def print_select_statements(node_dict, children_dict):

    table_id = create_identifier(db=INPUT["db"], table=INPUT["table"])

    V = set( node_dict.keys() )
    E = { k: conv_to_repr(v) for k, v in children_dict.items() }
    G = {"V":V, "E":E}

    nodes_and_queries = ga.graphBFS(G=G, start=table_id, get_adj_func=ga.get_adjacent_dict )
    queries = nodes_and_queries[1]

    with open("tttttttt.csv", "w") as otfl:
        wt = csv.writer(otfl, delimiter="\t")
        wt.writerow(["query"])
        wt.writerows(queries)


# --------------------- Util Functions ----------------------

def get_val_from_matching_key(key, dct):
    return 


def conv_to_repr(lst):
    return [ repr(k) for k in lst ]



node_dict, parents_dict, children_dict = create_table_classes()
print_select_statements(node_dict, children_dict)


