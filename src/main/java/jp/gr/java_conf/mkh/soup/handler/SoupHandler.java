package jp.gr.java_conf.mkh.soup.handler;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import jp.gr.java_conf.mkh.soup.entity.Game;
import jp.gr.java_conf.mkh.soup.entity.Story;

import java.util.*;

public class SoupHandler implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent>{

    private Gson gson = new Gson();
    private DynamoDB story;
    private static final String DB_NAME_STORY = "story";

    @Override
    public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent event, Context context) {
        return null;
    }

    public APIGatewayProxyResponseEvent startGame(APIGatewayProxyRequestEvent event, Context context)
    {
        LambdaLogger logger = context.getLogger();

        final AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
        final DynamoDBMapper mapper = new DynamoDBMapper(client);

        Map<String, String> param = gson.fromJson(event.getBody(), new TypeToken<Map<String, String >>() {}.getType());

        APIGatewayProxyResponseEvent response = new APIGatewayProxyResponseEvent();
        response.setIsBase64Encoded(false);
        response.setStatusCode(400);
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("Content-Type", "application/json");
        headers.put("Access-Control-Allow-Origin", "http://soup-game.s3-website-ap-northeast-1.amazonaws.com");
        response.setHeaders(headers);
        Map<String, String> body = new HashMap<>();
        body.put("message", "error!!");

        Game game = new Game();
        game.setId(param.get("gameId"));
        game.setQuestionList(new ArrayList<>());

        if (mapper.load(game) == null) {
            mapper.save(game);
            response.setStatusCode(200);
            body.put("message", "success");
        }

        response.setBody(gson.toJson(body));
        return response;
    }

    public APIGatewayProxyResponseEvent endGame(APIGatewayProxyRequestEvent event, Context context)
    {
        LambdaLogger logger = context.getLogger();

        final AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
        final DynamoDBMapper mapper = new DynamoDBMapper(client);

        Map<String, String> param = gson.fromJson(event.getBody(), new TypeToken<Map<String, String >>() {}.getType());

        APIGatewayProxyResponseEvent response = new APIGatewayProxyResponseEvent();
        response.setIsBase64Encoded(false);
        response.setStatusCode(400);
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("Content-Type", "application/json");
        headers.put("Access-Control-Allow-Origin", "http://soup-game.s3-website-ap-northeast-1.amazonaws.com");
        response.setHeaders(headers);
        response.setBody("");

        Game game = new Game();
        game.setId(param.get("gameId"));
        mapper.delete(game);
        response.setStatusCode(200);

        return response;
    }

    public APIGatewayProxyResponseEvent getQuestion(APIGatewayProxyRequestEvent event, Context context) {
        LambdaLogger logger = context.getLogger();

        APIGatewayProxyResponseEvent response = new APIGatewayProxyResponseEvent();
        response.setIsBase64Encoded(false);
        response.setStatusCode(400);
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("Content-Type", "application/json");
        headers.put("Access-Control-Allow-Origin", "http://soup-game.s3-website-ap-northeast-1.amazonaws.com");
        response.setHeaders(headers);
        response.setBody("");

        String gameId = event.getQueryStringParameters().get("gameId");

        final AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
        final DynamoDBMapper mapper = new DynamoDBMapper(client);

        Game searchGameCondition = new Game();
        searchGameCondition.setId(gameId);
        Game game = mapper.load(searchGameCondition);
        if (game == null) {
            Map<String, String> body = new HashMap<>();
            body.put("message", "Game ID:" + gameId + " dose not begin.");
            response.setBody(gson.toJson(body));
            return response;
        }
        List<Integer> questionList = game.getQuestionList();

        DynamoDBScanExpression scanExpression = new DynamoDBScanExpression()
                .withConsistentRead(false);
        List<Story> storyList = mapper.scan(Story.class, scanExpression);

        if (storyList.size() == questionList.size()) {
            Map<String, String> body = new HashMap<>();
            body.put("message", "There is no Questions.");
            response.setBody(gson.toJson(body));

            return response;
        }

        Set<Integer> storySet = new HashSet<>();
        List<Integer> modifiedList = new ArrayList<>();
        for (Story item : storyList) {
            storySet.add(item.getId());
        }
        for (Integer question : questionList) {
            storySet.remove(question);
            modifiedList.add(question);
        }

        Integer[] indexArray = storySet.toArray(new Integer[0]);
        Random rand = new Random();
        int index = indexArray[storySet.size() == 1 ? 0 : rand.nextInt(storySet.size() - 1)];
        Story story = storyList.get(index);

        modifiedList.add(index);
        game.setQuestionList(modifiedList);
        mapper.save(game);
        response.setStatusCode(200);
        response.setBody(gson.toJson(story));

        return response;
    }

}
