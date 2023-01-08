package mse.advDB;

import java.util.List;

public class Article {

  public String _id;

  public String title;

  public List<Author> authors;

  public List<String> references;

  public static class Author {
    public String _id;
    public String name;
  }

  public String toString() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append(_id).append(", ").append(title);
    stringBuilder.append("\t");
    if (authors != null) {

      for (Author author : authors) {
        if (author.name != null) {
          stringBuilder.append(author.name);
          stringBuilder.append(", ");
        }
      }
    }
    return stringBuilder.toString();
  }
}
