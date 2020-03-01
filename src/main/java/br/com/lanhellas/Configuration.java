package br.com.lanhellas;

public final class Configuration {
    private Configuration(){}

    public static final String APIKEY = System.getenv("ENV_APIKEY");
    public static final String SECRET = System.getenv("ENV_SECRET");
    public static final String BROKER_URL = System.getenv("ENV_BROKER_URL");
    public static final String INPUT_TOPIC = "input-topic";
    public static final String OUTPUT_TOPIC = "output-topic";
}
