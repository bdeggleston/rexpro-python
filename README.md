rexpro
======

rexpro-python is an experimental rexpro socket interface for python

this library relies on recent changes to the rexster rexpro protocol, so you'll need to clone and build the rexster master branch if you want to use

## Installation
```
pip install rexpro
```

## Basic Usage

```python
from rexpro import RexProConnection

#create a connection
conn = RexProConnection('localhost', 8184, 'emptygraph')

#create and return some elements
#execute takes a script, and optionally, parameter bindings
#for parameterized queries
elements = conn.execute(
    """
    def v1 = g.addVertex([prop:bound_param1]
    def v2 = g.addVertex([prop:bound_param2])
    def e = g.addEdge(v1, v2, 'connects', [prop:bound_param3])
    return [v1, v2, e]
    """,
    {'bound_param1':'b1', 'bound_param2':'b2', 'bound_param3':'b3'}
)
```

## Transactional Graphs

if you're using this with a transactional graph you can do requests in the context of a transaction one of two ways

```python
#first, by explicitly opening and closing a transaction
conn.open_transaction()
conn.execute("//do some stuff")
conn.close_transaction()

#second, with a context manager
with conn.transaction():
    conn.execute("//do some other stuff")
```


