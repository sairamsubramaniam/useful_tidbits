
from collections import defaultdict
import copy
import logging
import queue

import sqlalchemy as sa


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



def create_identifier(db, table, colname):
    return db + "___" + table + "___" + colname


class node(object):

    def __init__(self, db, table, colname, colvalues=[], 
                       parents=[], children=[]):
        self.db = db
        self.table = table
        self.colname = colname
        self.colvalues = colvalues
        self._level = None

        self._parents = parents
        self._children = children
        # print("-----------------------------------")
        # print("FOR: ", repr(self))
        # print("PARENT BEFORE: ", self._parents)
        # print("CHILDREN BEFORE: ", self._children)


    def __eq__(self, oth):
        if isinstance(oth, self.__class__):
            return oth.__dict__ == self.__dict__
        return False

    
    @property
    def parents(self):
        return self._parents


    @property
    def children(self):
        return self._children


    @property
    def level(self):
        return self._level


    @parents.getter
    def parents(self):
        return self._parents


    @children.getter
    def children(self):
        return self._children


    @level.getter
    def level(self):
        return self.level


    @parents.setter
    def parents(self, parents_list=[]):
        for k in parents_list:
            if k not in self._parents:
                self._parents.append(k)
        # print("PARENT AFTER: ", self._parents)



    @children.setter
    def children(self, children_list=[]):
        for k in children_list:
            if k not in self._children:
                self._children.append(k)
        # print("CHILDREN AFTER: ", self._children)
        # print("-----------------------------------")


    @level.setter
    def level(self, lev=None):
        if not self._parents:
            self.level = 0
        else:
            self.level = self._parents[0].level + 1


    def get_select_query(self):
        q = SELECT_QUERY_TEMPLATE.format(db=self.db,
                                         table=self.table,
                                         colname=self.colname)
        q_params = {"colvalues": self.colvalues}
        return q, q_params
    
    def get_delete_query(self):
        q = DELETE_QUERY_TEMPLATE.format(db=self.db,
                                         table=self.table,
                                         colname=self.colname)
        q_params = {"colvalues": self.colvalues}
        return q, q_params

    def __repr__(self):
        return create_identifier(self.db, self.table, self.colname)

    
    def __str__(self):
        return create_identifier(self.db, self.table, self.colname)



class BreadthFirstSearch(object):
    def __init__(self):
        pass





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
    graph = {}
    cntr = 0
    for row in resprox.fetchall():
        # print("Graph Beginning Of Loop: ", graph)
        r = dict(row)
        # print("ROW: ", r)

        child_dict, parent_dict = extract_child_parent_dict(row_dict=r)
        # node_c = node(**child_dict)
        node_p = node(**parent_dict)
        # node_c.parents = [node_p]
        node_p.children = [node(**child_dict)]
        
        p = create_identifier(db=parent_dict["db"], 
                              table=parent_dict["table"], 
                              colname=parent_dict["colname"]
                             )
        c = create_identifier(db=child_dict["db"], 
                              table=child_dict["table"], 
                              colname=child_dict["colname"]
                             )
        # if c not in graph:
        #     graph[c] = node_c
        #     print("1: ", graph)

        if p not in graph:
            graph[p] = node_p
            # print("2: ", graph)
        # print("Graph End Of Loop: ", graph)
    
    return graph


def add_levels_to_graph(graph):
    start_db = INPUT["db"]
    start_table = INPUT["table"]
    start_colname = INPUT["colname"]

    node_id = create_identifier(db=start_db, table=start_table, colname=start_colname)




graph = create_col_graph_without_levels()
for k, v in graph.items():
    print("******************")
    print(k, " - Parents: ", len(v.parents), ", Children: ", len(v.children))

# print(graph["wcore_dev_db___workflow_state___workflow_id"].children)
