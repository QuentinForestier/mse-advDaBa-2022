package mse.advDB;

import org.neo4j.driver.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class Writer implements Runnable
{
    private Driver driver;

    private LinkedList<Article> articles;


    Writer(Driver d, LinkedList<Article> articles)
    {
        driver = d;
        this.articles = articles;
    }

    private Map<String, Object> listAsMap()
    {
        LinkedList<Map<String, Object>> list = new LinkedList<>();
        for (Article article : articles)
        {
            list.add(article.toMap());
        }
        Map<String, Object> map = new HashMap<>();
        map.put("articles", list);
        return map;
    }

    private void write()
    {
        Map<String, Object> map = listAsMap();
        ArrayList<Query> queryList = new ArrayList<>();
        Query query = new Query("UNWIND $articles as article " +
                "WITH article " +
                "MERGE (a:Article {_id: article._id}) SET a.title = article" +
                ".title RETURN id(a)",
                map
        );

        queryList.add(query);
        Query referencesQuery = new Query(
                "UNWIND $articles as article " +
                        "WITH article " +
                        "UNWIND article.references AS reference " +
                        "WITH article, reference " +
                        "MATCH (a:Article " +
                        "{_id: article._id}) WITH a, reference MERGE " +
                        "(b:Article {_id: reference}) WITH a, b MERGE " +
                        "(a)-[:CITES]->(b)",
                map);

        queryList.add(referencesQuery);


        Query authorsQuery = new Query(
                "UNWIND $articles as article " +
                        "WITH article " +
                        "UNWIND article.authors as author " +
                        "WITH article, author " +
                        "MATCH (b:Article {_id: article._id}) " +
                        "WITH b, author " +
                        "MERGE (a:Author {_id: author._id}) " +
                        "SET a.name = author.name " +
                        "WITH a, b " +
                        "MERGE (a)-[:AUTHORED]->(b)",
                map);
        queryList.add(authorsQuery);


        try (Session session = driver.session(SessionConfig.defaultConfig()))
        {
            session.writeTransaction(tx ->
            {
                for (Query q : queryList)
                {
                    tx.run(q);
                }
                return 1;
            });
        }
    }

    @Override
    public void run()
    {
        write();
    }
}
