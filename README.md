# **JACLANG JASECI**

### Installation
```bash
pip install -e .
```
### Dependencies for Devs
```bash
pip install black pre-commit pytest flake8 flake8_import_order flake8_docstrings flake8_comprehensions flake8_bugbear flake8_annotations pep8_naming flake8_simplify mypy pytest
pre-commit install
```

## `HOW TO USE`

### **RUNNER**
```python
import:py from jaclang_jaseci, start; # runner import

with entry:__main__ {
    start(  # runner trigger
        host="0.0.0.0",
        port=8000
    );
}
```
---
---
```
```
### **Walker API**
- requires @specs decorator or inner class Specs
- specs defaults to path = "", methods = ["post"], as_query = [], auth = true
- walker endpoints will generate multiple endpoints
  - /walker/walker_name (root entry)
  - /walker/walker_name/{node-id} (specified node_id entry)
```python
# runner import ...
import:py from jaclang_jaseci.plugins, specs;

# this will be ignored in api generation
# @specs or inner obj Specs is required
walker non_api {
    # ... your code ...
}

walker sample_api {
    # ... your code ...
    # Specs defaults to path = "", methods = ["post"], as_query = [], auth = true
    obj Specs {}
    # ... your code ...
}

# Specs defaults to path = "", methods = ["post"], as_query = [], auth = true
@specs
walker sample_api_via_decorator {
    # ... your code ...
}

# runner trigger ...
```

#### **Different Walker Options**
##### `GET`
```python
# runner import ...
import:py from jaclang_jaseci.plugins, specs;

walker get_api {
    obj Specs {
        static has methods = ["get"];
    }
}

@specs(methods = ["get"])
walker get_api_via_decorator {}

# runner trigger ...
```
##### `GET WITH QUERY PARAMS`
```python
# runner import ...
import:py from jaclang_jaseci.plugins, specs;

walker get_api_with_query_params {
    has param: str;

    obj Specs {
        static has methods = ["get"], as_query = ["param"];
    }
}

@specs(methods = ["get"], as_query = ["param"])
walker get_api_with_query_params_via_decorator {
    has param: str;
}

# runner trigger ...
```
##### `GET ALL QUERY PARAMS`
```python
# runner import ...
import:py from jaclang_jaseci.plugins, specs;

walker get_api {
    has query_field1: str; # query params
    has query_field2: str; # query params
    obj Specs {
        static has methods = ["get"], as_query = "*";
    }
}

@specs(methods = ["get"], as_query = "*")
walker get_api_via_decorator {
    has query_field1: str; # query params
    has query_field2: str; # query params
}

# runner trigger ...
```
##### `GET WITH REQUEST BODY`
```python
# runner import ...
import:py from jaclang_jaseci.plugins, specs;

walker get_api_with_request_body {
    has param: str;

    obj Specs {
        static has methods = ["get"];
    }
}

@specs(methods = ["get"])
walker get_api_with_request_body_via_decorator {
    has param: str;
}

# runner trigger ...
```
##### `GET WITH QUERY PARAM AND REQUEST BODY`
```python
# runner import ...
import:py from jaclang_jaseci.plugins, specs;

walker get_api_with_query_params_and_request_body {
    has param1: str; # query params
    has param2: str; # json body

    obj Specs {
        static has methods = ["get"], as_query = ["param1"];
    }
}

@specs(methods = ["get"], as_query = ["param1"])
walker get_api_with_query_params_and_request_body_via_decorator {
    has param1: str; # query params
    has param2: str; # json body
}

# runner trigger ...
```
##### `POST`
```python
# runner import ...
import:py from jaclang_jaseci.plugins, specs;

walker post_api {
    obj Specs {}
}

@specs
walker post_api_via_decorator {}

# runner trigger ...
```
##### `POST WITH QUERY PARAMS`
```python
# runner import ...
import:py from jaclang_jaseci.plugins, specs;

walker post_api_with_query_params {
    has param: str;

    obj Specs {
        static has as_query = ["param"];
    }
}

@specs(as_query = ["param"])
walker post_api_with_query_params_via_decorator {
    has param: str;
}

# runner trigger ...
```
##### `POST WITH REQUEST BODY`
```python
# runner import ...
import:py from jaclang_jaseci.plugins, specs;

walker post_api_with_request_body {
    has param: str;

    obj Specs {}
}

@specs
walker post_api_with_request_body_via_decorator {
    has param: str;
}

# runner trigger ...
```
##### `POST WITH QUERY PARAM AND REQUEST BODY`
```python
# runner import ...
import:py from jaclang_jaseci.plugins, specs;

walker post_api_with_query_params_and_request_body {
    has param1: str; # query params
    has param2: str; # json body

    obj Specs {
        static has as_query = ["param1"];
    }
}

@specs(as_query = ["param1"])
walker post_api_with_query_params_and_request_body_via_decorator {
    has param1: str; # query params
    has param2: str; # json body
}

# runner trigger ...
```
##### `PATH VARIABLE`
```python
# runner import ...
import:py from jaclang_jaseci.plugins, specs;

walker post_api_with_path_variable {
    has param: str; # path variable

    obj Specs {
        static has path = "/{param}";
    }
}

@specs(path = "/{param}")
walker post_api_with_path_variable_decorator {
    has param: str; # path variable
}

# runner trigger ...
```
##### `COMBINATION WITH ALL SUPPORTED OF METHODS`
```python
# runner import ...
import:py from jaclang_jaseci.plugins, specs;

walker post_api_with_path_variable {
    has path_variable: str; # path variable
    has json_field: int;
    has query_field: str;

    obj Specs {
        static has path = "/{path_variable}", methods = ["post", "get", "put", "patch", "delete", "head", "trace", "options"], as_query = ["query_field"];
    }
}

@specs(path = "/{param}")
walker post_api_with_path_variable_via_decorator {
    has param: str; # path variable
}

# runner trigger ...
```
##### `FILE UPLOAD`
- This will automatically set to accept multipart/form-data
```python
# runner import ...
import:py from jaclang_jaseci.plugins, specs;
import:py from jaclang_jaseci.types, File, Files, OptFile, OptFiles;

walker post_with_file {
    has single: File;
    has multiple: Files;
    has singleOptional: OptFile = None;
    has multipleOptional: OptFiles = None;

    obj Specs {}
}

@specs
walker post_with_file_via_decorator {
    has single: File;
    has multiple: Files;
    has singleOptional: OptFile = None;
    has multipleOptional: OptFiles = None;
}

# runner trigger ...
```
---
```
```
### **Node**
- Nodes will be automatically set as model.
```python
node sample {
    has val: int = 1;
}
```
```js
// database entry equivalent
{
    "_id": ObjectId(),
    "name": "sample",
    "root": ObjectId() of root node,
    "access": {
        "all": bool,
        "nodes": [
            [ /* list of node's ObjectId() that has read access to this node */ ],
            [ /* list of node's ObjectId() that has write access to this node */ ]
        ],
        "roots": [
            [ /* list of root's ObjectId() that has read access to this node */ ],
            [ /* list of root's ObjectId() that has write access to this node */ ]
        ]
    },
    "context": {
        "val": 1
    }
}
```
##### `NEW NODE SAVE`
- if node is newly created and trigger save. It will save the whole object.
```python
can ability_name ...{your options}... {
    node1 = sample();
    node1.save();
}
```
##### `EXISTING NODE SAVE`
- if node is already existing and trigger save. It will only save fields that has update. In this case only `context.val` will be updated.
```python
can ability_name with sample entry {
    here.val = 2;
    node1.save();
}
```
##### `CONNECTING MULTIPLE NEW NODES WITH SAVE`
- node save will propagate to all it's adjacent nodes that's not yet existing in DB. This is to make sure that the reference of edges is always existing in database.
- if you have trigger multiple save, it will only process the save once per node. If node is already updated and for some reason save is triggered again, it will always check if there's changes happened on the node. If it doesn't find any, it will just ignore the save.
```python
can ability_name1 with `root entry {
    a = sample();
    b = sample();

    a ++> b;
    a.save(); # this will trigger save for both a and b

    b.save(); # will be ignored.
}

can ability_name2 with `root entry {
    a = sample();
    b = sample();

    a ++> b;
    a.save(); # this will trigger save for both a and b

    b.val = 4;
    b.save(); # will save context.val = 4
}
```
##### `CONNECTING MULTIPLE NEW OR EXISTING NODES WITH SAVE`
```python
can ability_name with sample entry {
    a = sample();
    b = sample();

    here ++> a;
    a ++> b;
    a.save(); # will only propagate to b and not in here node.
    b.save(); # will be ignored. Already saved

    here.save(); # since here node is already existing, it must trigger save manually.
}
```
##### `DESTROY NODES`
```python
can ability_name with sample entry {
    a = sample();

    a.destroy(); # will be ignored since it's not existing on db

    here.destroy(); # will delete here node, all edges connected to it and remove adjacent node's reference to this node.
    # in context, it trigger multiple operation
}
```
### **User**
- override User fields
```python
::py::
NULL_BYTES = bytes()

class User(BaseUser):
    name: str # additional name field

    class Collection(BaseUser.Collection):
        @classmethod
        def __document__(cls, doc) -> "User": # override parser
            return User.model()(
                id=str(doc.pop("_id")),
                email=doc.pop("email"),
                password=doc.pop("password", None) or NULL_BYTES,
                root_id=str(doc.pop("root_id")),
                **doc,
            )

    # override emailer
    @staticmethod
    def send_verification_code(code: str, email: str) -> None:
        """Send verification code."""
        SendGridEmailer.send_verification_code(code, email)
::py::
```
### **Controlling Access**
```python
# syntax to get doc_anchor from string id
doc_anchor = DocAnchor.ref("your target id");

# will allow read access from node1 to node2 - node1 can connect to node2 but can't update any field
node2.allow_node(docAnchor_of_node1);

# will allow write access from node1 to node2 - node1 can connect to node2 and update it's field
node2.allow_node(docAnchor_of_node1, True);

# will remove all access from node1
node2.disallow_node(docAnchor_of_node1);

# will allow read access from any node from root1 to node2 - any node from root1 can connect to node2 but can't update any field
node2.allow_root(docAnchor_of_root1);

# will allow write access from any node from root1 to node2 - any node from root1 can connect to node2 and update it's field
node2.allow_root(docAnchor_of_root1, True);

# will remove all access from root1
node2.disallow_root(docAnchor_of_root1);

# will allow read access from node1 to any node from root2- node1 can connect to any node from root2 but can't update any field
root2.allow_node(docAnchor_of_node1);

# will allow write access from node1 to any node from root2- node1 can connect to any node from root2 and update it's field
root2.allow_node(docAnchor_of_node1, True);

# will remove all access from node1
root2.disallow_node(docAnchor_of_node1);

# will allow read access from any node from root1 to any node from root2 - any node from root1 can connect to any node from root2 but can't update any field
root2.allow_root(docAnchor_of_root1);

# will allow write access from any node from root1 to any node from root2 - any node from root1 can connect to any node from root2 and update it's fields root2.disallow_root(docAnchor_of_root1)# will remove all access from root1
root2.allow_root(docAnchor_of_root1, True);

# allow all read access from any node/root to node2
node2.unrestrict();

# allow all write access from any node/root to node2
node2.unrestrict(True);

# back to normal and subject for node/root access validation for to node2
node2.restrict();

# allow all read access from any node/root to any node from root2
root2.unrestrict();

# allow all write access from any node/root to any node from root2
root2.unrestrict(True);

# back to normal and subject for node/root access validation for all nodes from root2
root2.restrict();
```