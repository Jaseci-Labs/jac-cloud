"""Example of simple walker walking nodes."""
import:py from jaclang_jaseci, FastAPI, start;
import:py from jaclang_jaseci.types, File, Files, OptFile, OptFiles, Defaults;
import:py from jaclang_jaseci.models, User as BaseUser;
import:py from jaclang_jaseci.plugins, specs, DocAnchor;

# if possible to support this native class declaration
::py::
NULL_BYTES = bytes()

class User(BaseUser):
    name: str

    class Collection(BaseUser.Collection):
        @classmethod
        def __document__(cls, doc) -> "User":
            return User.model()(
                id=str(doc.pop("_id")),
                email=doc.pop("email"),
                password=doc.pop("password", None) or NULL_BYTES,
                root_id=str(doc.pop("root_id")),
                **doc,
            )
::py::

node boy {
    has val1: str, val2: str;
}

node girl {
    has val: str;
}

node someone {
    has val: list = [];
}

@specs(path = "will-be-ignored-because-of-inner-class-Specs")
walker create_sample_graph {
    async can enter with `root entry {
        b = boy(val1="a", val2="b");
        g = girl(val="b");
        b ++> g;
        root ++> b;
        await root.save();
        return True;
    }

    obj Specs {}
}

@specs
walker visit_sample_graph {
    async can enter1 with `root entry {
        report root;
        boys = await [-->];
        report boys[0];
        visit boys;
        return 1;
    }

    async can enter2 with boy entry {
        girls = await [-->];
        report girls[0];
        visit girls;
        return 2;
    }

    async can enter3 with girl entry {
        anyone = await [-->];
        report anyone;
        visit anyone;
        return 3;
    }
}

walker update_sample_graph {
    async can enter1 with `root entry {
        boys = await [-->];
        visit boys;
    }

    async can enter2 with boy entry {
        old = await [-->];
        report old[0];

        new = girl(val="new");
        here ++> new;
        await new.save();

        await old[0].destroy();

        report (await [-->])[0];
    }

    obj Specs {}
}

walker create_list_field {
    async can enter1 with `root entry {
        a = someone();
        root ++> a;
        await root.save();
        report a;
    }
    obj Specs {}
}

walker update_list_field {
    async can enter2 with someone entry {
        here.val.append(1);
        await here.save();
        report here;

        visit await [-->];
    }

    obj Specs {}
}

walker get_list_field {
    async can enter2 with someone entry {
        report here;
        report await [-->];
    }

    obj Specs {}
}

walker allow_other {
    has root_id: str = None, node_id: str = None, write: bool = False;

    async can enter2 with someone entry {
        if self.root_id {
            here.allow_root(DocAnchor.ref(self.root_id), self.write);
        }
        if self.node_id {
            here.allow_node(DocAnchor.ref(self.node_id), self.write);
        }
        await here.save();
        report here;
    }

    obj Specs {}
}

walker disallow_other {
    has root_id: str = None, node_id: str = None;

    async can enter2 with someone entry {
        if self.root_id {
            here.disallow_root(DocAnchor.ref(self.root_id));
        }
        if self.node_id {
            here.disallow_node(DocAnchor.ref(self.node_id));
        }

        await here.save();
        report here;
    }

    obj Specs {}
}

walker connect_other_node {
    has node_id: str = None;

    async can enter2 with someone entry {
        if self.node_id {
            other_node = await DocAnchor.ref(self.node_id).connect(here);
            here ++> other_node;
            await here.save();
        }
        report here;
    }

    obj Specs {}
}

@specs
walker post_no_body {}

walker post_with_body {
    has a: str;

    obj Specs {}
}

walker get_no_body {
    obj Specs {
        static has methods: list = ["get"];
    }
}

walker get_with_query {
    has a: str;

    obj Specs {
        static has methods: list = ["get"], as_query: list = ["a"];
    }
}

walker get_all_query {
    has a: str;
    has b: str;

    obj Specs {
        static has methods: list = ["get"], as_query: list = "*", auth: bool = False;
    }
}

walker combination {
    has a: str;
    has b: str;
    has c: str;

    obj Specs {
        static has methods: list = ["post", "get"], as_query: list = ["a", "b"];
    }
}

walker post_path_var {
    has a: str;

    obj Specs {
        static has path: str = "/{a}", methods: list = ["post", "get"];
    }
}

walker combination2 {
    has a: str;
    has b: str;
    has c: str;

    obj Specs {
        static has path: str = "/{a}", methods: list = ["post", "get", "put", "patch", "delete", "head", "trace", "options"], as_query: list = ["b"];
    }
}

walker post_with_file {
    has single: File;
    has multiple: Files;
    has singleOptional: OptFile = None;
    has multipleOptional: OptFiles = None;


    async can enter with `root entry {
        print(self.single);
        print(self.multiple);
        print(self.singleOptional);
        print(self.multipleOptional);
    }

    obj Specs {}
}

walker post_with_body_and_file {
    has val: int;
    has single: File;
    has multiple: Files;

    obj Specs {}
}

with entry:__main__ {
    start(
        host="0.0.0.0",
        port=8000
    );
}