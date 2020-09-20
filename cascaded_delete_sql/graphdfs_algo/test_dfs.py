
from graphdfs_algo import graphdfs_algo as ga

graphDFS = ga.graphDFS


V = {1,2,3,4,5,6}
E = {(1,2),(1,3),(2,4),(2,5),(3,5),(3,6) }
G = {"V":V, "E":E}



def test_all():
    pth1to1 = graphDFS(G=G, start=1, goal=1)
    pth1to2 = graphDFS(G=G, start=1, goal=2)
    pth1to3 = graphDFS(G=G, start=1, goal=3)
    pth1to4 = graphDFS(G=G, start=1, goal=4)
    pth1to5 = graphDFS(G=G, start=1, goal=5)
    pth1to6 = graphDFS(G=G, start=1, goal=6)
    pth2to2 = graphDFS(G=G, start=2, goal=2)
    pth2to3 = graphDFS(G=G, start=2, goal=3)
    pth2to4 = graphDFS(G=G, start=2, goal=4)
    pth2to5 = graphDFS(G=G, start=2, goal=5)
    pth2to6 = graphDFS(G=G, start=2, goal=6)
    pth3to3 = graphDFS(G=G, start=3, goal=3)
    pth3to4 = graphDFS(G=G, start=3, goal=4)
    pth3to5 = graphDFS(G=G, start=3, goal=5)
    pth3to6 = graphDFS(G=G, start=3, goal=6)
    pth4to4 = graphDFS(G=G, start=4, goal=4)
    pth4to5 = graphDFS(G=G, start=4, goal=5)
    pth4to6 = graphDFS(G=G, start=4, goal=6)
    pth5to5 = graphDFS(G=G, start=5, goal=5)
    pth5to6 = graphDFS(G=G, start=5, goal=6)
    pth6to6 = graphDFS(G=G, start=6, goal=6)

    assert (pth1to1[0]==1) and (pth1to1[-1]==1)
    assert (pth1to2[0]==1) and (pth1to2[-1]==2)
    assert (pth1to3[0]==1) and (pth1to3[-1]==3)
    assert (pth1to4[0]==1) and (pth1to4[-1]==4)
    assert (pth1to5[0]==1) and (pth1to5[-1]==5)
    assert (pth1to6[0]==1) and (pth1to6[-1]==6)
    assert (pth2to2[0]==2) and (pth2to2[-1]==2)
    assert (pth2to3==[])
    assert (pth2to4[0]==2) and (pth2to4[-1]==4)
    assert (pth2to5[0]==2) and (pth2to5[-1]==5)
    assert (pth2to6==[])
    assert (pth3to3[0]==3) and (pth3to3[-1]==3)
    assert (pth3to4==[])
    assert (pth3to5[0]==3) and (pth3to5[-1]==5)
    assert (pth3to6[0]==3) and (pth3to6[-1]==6)
    assert (pth4to4[0]==4) and (pth4to4[-1]==4)
    assert (pth4to5==[])
    assert (pth4to6==[])
    assert (pth5to5[0]==5) and (pth5to5[-1]==5)
    assert (pth5to6==[])
    assert (pth6to6[0]==6) and (pth6to6[-1]==6)


