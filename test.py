from cachepipe import CachePipe,TABLENAME,TABLEINDEX,serdes

pipe = CachePipe(r"c:\temp2\testinghhh.db")
@pipe.func(ver=1,schema=[TABLENAME,TABLENAME])
def fnA(a, b)->str:
    return "result" + str(a) + str(b)

@pipe.func(ver=1,schema=[TABLENAME,TABLENAME],dependencies=[fnA])
def fuB(a, b)->str:
    x = other_func(a, b)
    return x

def other_func(a,b):
    return fnA(a, b)


@pipe.func(ver=1,schema=[TABLENAME,TABLEINDEX],dependencies=[fuB])
def fuC(a, b)->str:
    x = fuB(a, b)
    return x


@pipe.func(ver=1,schema=[TABLENAME,TABLEINDEX],dependencies=[fuC])
def fuD(a, b)->str:
    x = fuC(a, b)
    return x

@pipe.func(ver=1,schema=[TABLENAME,TABLEINDEX],dependencies=[fuD])
def fuE(a, b)->str:
    x = fuD(a, b)
    return x

@pipe.func(ver=1,schema=[TABLENAME,TABLEINDEX],dependencies=[fuE])
def fuF(a, b)->str:
    x = fuE(a, b)
    return x

@pipe.func(ver=1,schema=[TABLENAME,TABLEINDEX],dependencies=[fuF])
def fuG(a, b)->str:
    x = fuF(a, b)
    return x


@pipe.func(ver=1,schema=[TABLENAME,TABLEINDEX],dependencies=[fuG])
def fuH(a, b)->str:
    x = fuG(a, b)
    return x


@pipe.func(ver=1,schema=[TABLENAME,TABLENAME],dependencies=[fuH])
def fuI(a, b)->str:
    x = fuH(a, b)
    return x
print(fuB(1,2))