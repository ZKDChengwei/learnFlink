import com.google.gson.Gson;

public class t1 {
    public static void main(String[] args) {
        info ctx = new info("ctx");
        student bob = new student("Bob", 20, ctx);
        String s = "sdsdas";
        String a = "MM";


        Gson gson = new Gson();
        String json = gson.toJson(s+a);
        System.out.println(json);
    }
}

class student{
    String name;
    int age;
    info information;

    public student() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public info getInformation() {
        return information;
    }

    public void setInformation(info information) {
        this.information = information;
    }

    public student(String name, int age, info information) {
        this.name = name;
        this.age = age;
        this.information = information;
    }
}
class info{
    String ctx;

    public info(String ctx) {
        this.ctx = ctx;
    }

    public String getCtx() {
        return ctx;
    }

    public void setCtx(String ctx) {
        this.ctx = ctx;
    }

    public info() {
    }
}
