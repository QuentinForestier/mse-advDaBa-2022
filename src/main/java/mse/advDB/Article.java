package mse.advDB;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class Article
{

    public String _id;

    public String title;

    public List<Author> authors;

    public List<String> references;

    public static class Author
    {
        public String _id;
        public String name;
    }

    public String toString()
    {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(_id).append(", ").append(title);
        stringBuilder.append("\t");
        if (authors != null)
        {

            for (Author author : authors)
            {
                if (author.name != null)
                {
                    stringBuilder.append(author.name);
                    stringBuilder.append(", ");
                }
            }
        }
        return stringBuilder.toString();
    }

    public Map<String, Object> toMap()
    {
        Map<String, Object> map = new HashMap<>();
        map.put("_id", _id);
        map.put("title", title == null ? "" : title);

        List<Map<String, Object>> authorsList = new LinkedList<>();
        if (authors != null)
        {
            for (Author a : authors)
            {
                Map<String, Object> tmp = new HashMap<>();
                tmp.put("_id", a._id);
                tmp.put("name", a.name);
                authorsList.add(tmp);
            }
        }

        map.put("authors", authorsList);

        if (references != null)
        {
            List<String> filter = new LinkedList<>();
            filter.add("");
            references.removeAll(filter);
            map.put("references", references);

        }


        return map;
    }
}
