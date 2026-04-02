public class HocJava {
    int a = 10;
    int b = 20; 
    
    int c = a + b;

    public static void main(String[] args) {
        HocJava obj = new HocJava();
        obj.a = obj.a + 5;
        obj.b = obj.b + 10;

        System.out.println("a + b + 15 = " + (obj.a + obj.b));
        System.out.println("Hello, Java!" + "a + b = " + obj.getSum());
    }

    int getSum() {
        return a + b;
    }

}