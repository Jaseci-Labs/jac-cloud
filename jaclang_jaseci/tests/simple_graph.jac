"""Example of simple walker walking nodes."""
import:py from jaclang_jaseci, FastAPI;
import:py from jaclang_jaseci.plugin, specs;
import:py from jaclang_jaseci.jaseci.models, User as BaseUser, NO_PASSWORD;
import:py from jaclang_jaseci.jaseci.utils, SendGridEmailer;

::py::
class User(BaseUser):
    name: str

    class Collection(BaseUser.Collection):
        @classmethod
        def __document__(cls, doc) -> "User":
            return User.model()(
                id=str(doc.pop("_id")),
                email=doc.pop("email"),
                password=doc.pop("password", None) or NO_PASSWORD,
                root_id=str(doc.pop("root_id")),
                **doc,
            )

    @staticmethod
    def send_verification_code(code: str, email: str) -> None:
        """Send verification code."""
        SendGridEmailer.send_verification_code(code, email)

    @staticmethod
    def sso_mapper(open_id) -> dict[str, object]:
        """Send verification code."""
        return {
            "name": f"{open_id.first_name} {open_id.last_name}"
        }
::py::

node A {
    has val: int;
}

node B {
    has val: int;
}

node C {
    has val: int;
}

walker create_graph {
    can enter_root with `root entry {
        a = A(val=1);
        b = B(val=2);
        c = C(val=3);
        here ++> a;
        a ++> b;
        b ++> c;
        
        report here;
        visit [];
    }

    can enter_A with A entry {
        report here;
        visit [-->];
    }

    can enter_B with B entry {
        report here;
        visit [-->];
    }

    can enter_C with C entry {
        report here;
    }

    obj __specs__ {}
}

walker traverse_graph {
    can enter with `root entry {
        report here;
        visit [-->];
    }

    can enter_A with A entry {
        report here;
        visit [-->];
    }

    can enter_B with B entry {
        report here;
        visit [-->];
    }

    can enter_C with C entry {
        report here;
    }

    obj __specs__ {}
}

with entry:__main__ {
    # SendGridEmailer.start();
    FastAPI.start(
        host="0.0.0.0",
        port=8001
    );
}