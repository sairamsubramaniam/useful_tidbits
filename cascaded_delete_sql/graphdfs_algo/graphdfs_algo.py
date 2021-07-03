

import queue


V = {1,2,3,4,5,6}
E = {(1,2),(1,3),(2,4),(2,5),(3,5),(3,6) }
G = {"V":V, "E":E}



def get_adjacent_tuples(E, vertex):
    adjacent_vertices = []
    for edge in E:
        if edge[0] == vertex:
            adjacent_vertices.append(edge[1])
    return adjacent_vertices



def get_adjacent_dict(E, vertex):
    adjacent_vertices = E.get(vertex, [])
    return adjacent_vertices



def graphDFS(G, start, get_adj_func, goal=None):
    path = []
    V, E = G["V"], G["E"]
    stack = queue.LifoQueue()
    visited = set()
    stack.put(start)
    
    while not stack.empty():
        current = stack.get()
        path.append(current)
        visited.add(current)
        
        if current == goal:
            return path
        
        adjacent_vertices = get_adj_func(E=E, vertex=current)
        for v in adjacent_vertices:
            if v not in visited:
                stack.put(v)
        
    return []



def graphBFS(G, start, get_adj_func, goal=None):
    path = []
    V, E = G["V"], G["E"]
    stack = queue.Queue()
    visited = set()
    stack.put(start)
    
    while not stack.empty():
        current = stack.get()
        path.append(current)
        visited.add(current)
        
        if current == goal:
            return path
        
        adjacent_vertices = get_adj_func(E=E, vertex=current)
        for v in adjacent_vertices:
            if v not in visited:
                stack.put(v)
    
    if not goal:
        return path

    return []




# pth1to1 = graphDFS(G=G, start=1, goal=1)
# pth1to2 = graphDFS(G=G, start=1, goal=2)
# pth1to3 = graphDFS(G=G, start=1, goal=3)
# pth1to4 = graphDFS(G=G, start=1, goal=4)
# pth1to5 = graphDFS(G=G, start=1, goal=5)
# pth1to6 = graphDFS(G=G, start=1, goal=6)
# pth2to2 = graphDFS(G=G, start=2, goal=2)
# pth2to3 = graphDFS(G=G, start=2, goal=3)
# pth2to4 = graphDFS(G=G, start=2, goal=4)
# pth2to5 = graphDFS(G=G, start=2, goal=5)
# pth2to6 = graphDFS(G=G, start=2, goal=6)
# pth3to3 = graphDFS(G=G, start=3, goal=3)
# pth3to4 = graphDFS(G=G, start=3, goal=4)
# pth3to5 = graphDFS(G=G, start=3, goal=5)
# pth3to6 = graphDFS(G=G, start=3, goal=6)
# pth4to4 = graphDFS(G=G, start=4, goal=4)
# pth4to5 = graphDFS(G=G, start=4, goal=5)
# pth4to6 = graphDFS(G=G, start=4, goal=6)
# pth5to5 = graphDFS(G=G, start=5, goal=5)
# pth5to6 = graphDFS(G=G, start=5, goal=6)
# pth6to6 = graphDFS(G=G, start=6, goal=6)

