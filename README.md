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
    def v1 = g.addVertex([prop:bound_param1])
    def v2 = g.addVertex([prop:bound_param2])
    def e = g.addEdge(v1, v2, 'connects', [prop:bound_param3])
    return [v1, v2, e]
    """,
    {'bound_param1':'b1', 'bound_param2':'b2', 'bound_param3':'b3'}
)

#the contents of elements will be:
({'_id': '0', '_properties': {'prop': 'b1'}, '_type': 'vertex'},
 {'_id': '1', '_properties': {'prop': 'b2'}, '_type': 'vertex'},
 {'_id': '2',
  '_inV': '1',
  '_label': 'connects',
  '_outV': '0',
  '_properties': {'prop': 'b3'},
  '_type': 'edge'})
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

## Query scoping & global variables

A RexPro connection is basically a connection to a gremlin REPL.
Queries executed with the RexProConnection's `execute` method are automatically wrapped in a closure before being executed
to avoid cluttering the global namespace with variables defined in previous queries. A globally available `g` graph object
is is automatically defined at the beginning of a RexPro session.

If you would like to define additional global variables, don't define variables with a def statement. For example:

```python
#number will become a global variable for this session
conn.execute("number = 5")

#another_number is only available for this query
conn.execute("def another_number = 6")
```

