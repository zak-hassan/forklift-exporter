package dataformats.model;

public class CustomerBean {

  public int id;
  public String name;

  public CustomerBean(int id, String name) {
      this.id = id;
      this.name = name;
  }

  public int getId() {
      return id;
  }

  public String getName() {
      return name;
  }

}
