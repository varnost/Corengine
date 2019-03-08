package io.varnost.base;

import java.util.List;

public class Alert {
    public List<String> uuids;
    public String name;
    public String shortDesc;
    public String desc;

    public Alert(String n, String sd, String d, List<String> ids) {
        name = n;
        shortDesc = sd;
        desc = d;
        uuids = ids;
    }
}
