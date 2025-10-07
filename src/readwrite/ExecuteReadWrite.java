package readwrite;

public class ExecuteReadWrite {
    public static void main(String[] args){
        try {
            new WriteToHadoop().writeToHadoop(args[0]);
        }
        catch (Exception e) {
            System.out.println("You encounter IO exception while writing");
        }

        try {
            new ReadFromHadoop().readFromFile(args[0]);
        }
        catch (Exception e) {
            System.out.println("You encounter IO exception while reading");
        }
    }
}