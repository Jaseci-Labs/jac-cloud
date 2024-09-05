"""Example of simple walker walking nodes."""

import:py from jaclang_jaseci.plugins, specs;

walker sample {
    can enter with `root entry {
        wlk1 = here spawn nested1();
        wlk2 = here spawn nested2();
        wlk3 = here spawn nested3();
        print(wlk1.sample);
        print(wlk2.sample);
        print(wlk3.sample);
        print("sample");
    }

    obj Specs {
        static has auth: bool = False;
    }
}

walker nested1 {
    has sample: str = "";
    can enter with `root entry {
        self.sample = "nested1";
    }

    obj Specs {}
}

@specs
walker nested2 {
    has sample: str = "";
    can enter with `root entry {
        self.sample = "nested2";
    }
}

walker nested3 {
    has sample: str = "";
    can enter with `root entry {
        self.sample = "nested3";
    }
}

with entry:__main__ {
    root spawn sample();
}