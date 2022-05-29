package jp.gr.java_conf.mkh.soup.entity;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;

import java.util.ArrayList;
import java.util.List;

@DynamoDBTable(tableName = "game")
public class Game {

    String id;
    ArrayList<Integer> questionList;

    @DynamoDBHashKey(attributeName = "game_id")
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @DynamoDBAttribute(attributeName = "question_list")
    public List<Integer> getQuestionList() {
        return questionList;
    }

    public void setQuestionList(List<Integer> questionList) {
        this.questionList = (ArrayList<Integer>) questionList;
    }
}
