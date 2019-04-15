package io.varnost.base;

import java.util.List;

public class Alert {
    public List<String> uuids;
    public String name;
    public String shortDesc;
    public String desc;
    public String esQuery;

    public Alert(String n, String sd, String d, List<String> ids) {
        name = n;
        shortDesc = sd;
        desc = d;
        uuids = ids;
    }

    public void createESQuery() {
        String query = "uuid: (";
        for (String uuid : uuids) {
            query += '"' + uuid + '"' + " OR ";
        }
        query = query.substring(0, query.length() - 4);
        query += ")";
        esQuery = query;
    }
}
