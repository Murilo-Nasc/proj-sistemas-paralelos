public final class Main {
    private Main() {}

    public static void main(String[] args) throws Exception {
        String modo = args.length > 0 ? args[0] : "";
        switch (modo) {
            case "servidor":
                Servidor.executar();
                break;
            case "cliente":
                Cliente.executar();
                break;
            default:
                System.err.println("uso: java -jar app.jar <servidor|cliente>");
                System.exit(2);
        }
    }
}
