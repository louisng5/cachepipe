from cachepipe import CachePipe,TABLENAME,TABLEINDEX,serdes

pipe = CachePipe("testing.db")
@pipe.func(ver=1,schema=[TABLENAME,TABLENAME])
def fnA(a, b)->str:
    return "result" + str(a) + str(b)

@pipe.func(ver=1,schema=[TABLENAME,TABLEINDEX],dependencies=[fnA])
def fuB(a, b)->str:
    x = other_func(a, b)
    0/0
    return x

def other_func(a,b):
    return fnA(a, b)

print(fuB(1,2))