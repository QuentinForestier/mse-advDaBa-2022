package mse.advDB;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import mse.advDB.Article.Author;
import org.neo4j.driver.*;
import org.neo4j.driver.exceptions.ClientException;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class Example
{

    public static void main(String[] args) throws IOException,
            InterruptedException
    {
        int batchSize = Integer.parseInt(System.getenv("BATCH_SIZE"));

        String path = System.getenv("JSON_FILE");

        System.out.println("Path to json: " + path);

        int nbNode = Integer.max(1000, Integer.parseInt(System.getenv(
                "MAX_NODES")));
        String ip = System.getenv("NEO4J_IP");
        System.out.println("IP address of server : " + ip);


        Driver driver = GraphDatabase.driver("bolt://" + ip + ":7687",
                AuthTokens.basic("neo4j", "test"));
        boolean isConnected = false;
        do
        {
            try
            {
                System.out.println("Sleeping a bit waiting for the db");
                Thread.yield();

                Thread.sleep(5000); // let some time for the neo4j container
                // to be up and

                driver.verifyConnectivity();
                isConnected = true;
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        } while (!isConnected);

        JsonReader jsonReader = new JsonReader(new FileReader(path));
        jsonReader.setLenient(true);

        try (Session session = driver.session(SessionConfig.defaultConfig()))
        {
            session.writeTransaction(tx ->
            {
                try
                {
                    tx.run("CREATE CONSTRAINT ON (article:Article) ASSERT " +
                            "article._id IS UNIQUE");
                    tx.run("CREATE CONSTRAINT ON (author:Author) ASSERT " +
                            "author._id IS UNIQUE");
                }
                catch (Exception e)
                {
                    tx.rollback();
                }
                return 1;
            });
        }

        jsonReader.beginArray();
        int count = 0;
        LinkedList<Thread> threads = new LinkedList<>();
        while (jsonReader.hasNext() && count < nbNode && jsonReader.peek() != JsonToken.END_ARRAY)
        {
            // Create a list of ENV_NB_ARTICLES
            LinkedList<Article> articles = new LinkedList<>();
            int nbArticle = 0;
            while (nbArticle < batchSize && count < nbNode && jsonReader.hasNext() && jsonReader.peek() != JsonToken.END_ARRAY)
            {
                nbArticle++;

                // AJoute dans liste
                Article article = getNode(jsonReader);
                if (article.authors != null)
                {
                    count++;
                    article.authors = article.authors.stream()
                            .filter(author -> author._id != null)
                            .peek(author ->
                            {
                                if (author.name == null)
                                {
                                    author.name = "unknown";
                                }
                            }).collect(Collectors.toList());
                }
                articles.add(article);
            }


            Writer w = new Writer(driver, articles);// Give list and driver
            Thread t = new Thread(w);
            t.start();
            threads.add(t);
        }

        for(Thread t : threads){
            t.join();
        }

        driver.close();
        jsonReader.close();
    }

    private static List<String> getReferences(JsonReader jsonReader) throws IOException
    {
        List<String> refList = new ArrayList<>();
        jsonReader.beginArray();
        while (jsonReader.hasNext())
        {
            refList.add(jsonReader.nextString());
        }
        jsonReader.endArray();
        return refList;
    }

    private static Article getNode(JsonReader jsonReader) throws IOException
    {
        jsonReader.beginObject();
        Article node = new Article();
        while (jsonReader.hasNext())
        {
            String name = jsonReader.nextName();
            switch (name)
            {
                case "_id":
                    node._id = jsonReader.nextString();
                    break;
                case "title":
                    node.title = jsonReader.nextString();
                    break;
                case "authors":
                    node.authors = GetAuthorList(jsonReader);
                    break;
                case "references":
                    node.references = getReferences(jsonReader);
                    break;
                default:
                    jsonReader.skipValue();
                    break;
            }
        }
        jsonReader.endObject();
        return node;
    }

    private static Author getAuthor(JsonReader jsonReader) throws IOException
    {
        jsonReader.beginObject();
        Author author = new Author();
        while (jsonReader.hasNext())
        {
            String name = jsonReader.nextName();
            if (name.equals("_id"))
            {
                author._id = jsonReader.nextString();
            }
            else if (name.equals("name"))
            {
                author.name = jsonReader.nextString();
            }
            else
            {
                jsonReader.skipValue();
            }
        }
        jsonReader.endObject();
        return author;
    }

    private static List<Author> GetAuthorList(JsonReader jsonReader) throws IOException
    {
        List<Author> authorList = new ArrayList<>();
        jsonReader.beginArray();
        while (jsonReader.hasNext())
        {
            authorList.add(getAuthor(jsonReader));
        }
        jsonReader.endArray();
        return authorList;
    }
}