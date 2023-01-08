package mse.advDB;

import com.google.gson.stream.JsonReader;
import mse.advDB.Article.Author;
import org.neo4j.driver.*;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class Example {

    public static void main(String[] args) throws IOException {
        String path = System.getenv("JSON_FILE");

        System.out.println("Path to json: " + path);

        int nbNode = Integer.max(1000,Integer.parseInt(System.getenv("MAX_NODES")));
        String ip = System.getenv("NEO4J_IP");
        System.out.println("IP address of server : " + ip);

        String bufferFile = "temp.json";

        Driver driver = GraphDatabase.driver("bolt://" + ip + ":7687", AuthTokens.basic("neo4j", "test"));
        boolean isConnected = false;
        do {
            try {
                System.out.println("Sleeping a bit waiting for the db");
                Thread.yield();

                Thread.sleep(5000); // let some time for the neo4j container to be up and

                driver.verifyConnectivity();
                isConnected = true;
            } catch (Exception e) {
                e.printStackTrace();
            }
        } while (!isConnected);

        System.out.println("Currently loading data from " + bufferFile);

        JsonReader jsonReader = new JsonReader(new FileReader(path));
        jsonReader.setLenient(true);

        try (Session session = driver.session()) {
            int count = 0;
            jsonReader.beginArray();
            while (jsonReader.hasNext() && count < nbNode) {
                count++;
                Article article = getNode(jsonReader);
                if (article.authors != null) {
                    article.authors = article.authors.stream()
                            .filter(author -> author._id != null)
                            .peek(author -> {
                                if (author.name == null) {
                                    author.name = "unknown";
                                }
                            }).collect(Collectors.toList());
                }
                ArrayList<Query> queryList = new ArrayList<>();
                Query query = new Query("MERGE (a:Article {id: $id}) SET a.title = $title RETURN id(a)",
                        Values.parameters("title", article.title, "id", article._id));

                queryList.add(query);
                Query referencesQuery = new Query(
                        "UNWIND $references AS reference MATCH (a:Article {id: $id}) WITH a, reference MERGE (b:Article {id: reference}) WITH a, b MERGE (a)-[:CITES]->(b)",
                        Values.parameters("id", article._id, "references", article.references));

                queryList.add(referencesQuery);

                if (article.authors != null) {
                    for (Author author : article.authors) {
                        Query authorsQuery = new Query(
                                "MATCH (b:Article {id: $articleId}) WITH b MERGE (a:Author {id: $authorId}) SET a.name = $authorName WITH a, b MERGE (a)-[:AUTHORED]->(b)",
                                Values.parameters("articleId", article._id, "authorId", author._id, "authorName",
                                        author.name));
                        queryList.add(authorsQuery);
                    }
                }

                session.writeTransaction(tx -> {
                    for (Query q : queryList) {
                        tx.run(q);
                    }
                    return null;
                });
            }
        }
        jsonReader.close();
    }

    private static List<String> getReferences(JsonReader jsonReader) throws IOException {
        List<String> refList = new ArrayList<>();
        jsonReader.beginArray();
        while (jsonReader.hasNext()) {
            refList.add(jsonReader.nextString());
        }
        jsonReader.endArray();
        return refList;
    }

    private static Article getNode(JsonReader jsonReader) throws IOException {
        jsonReader.beginObject();
        Article node = new Article();
        while (jsonReader.hasNext()) {
            String name = jsonReader.nextName();
            switch (name) {
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

    private static Author getAuthor(JsonReader jsonReader) throws IOException {
        jsonReader.beginObject();
        Author author = new Author();
        while (jsonReader.hasNext()) {
            String name = jsonReader.nextName();
            if (name.equals("_id")) {
                author._id = jsonReader.nextString();
            } else if (name.equals("name")) {
                author.name = jsonReader.nextString();
            } else {
                jsonReader.skipValue();
            }
        }
        jsonReader.endObject();
        return author;
    }

    private static List<Author> GetAuthorList(JsonReader jsonReader) throws IOException {
        List<Author> authorList = new ArrayList<>();
        jsonReader.beginArray();
        while (jsonReader.hasNext()) {
            authorList.add(getAuthor(jsonReader));
        }
        jsonReader.endArray();
        return authorList;
    }
}