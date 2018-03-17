package myProj.WordPrediction;

import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DBOutputWritable implements DBWritable{
    private String starting_phrase;
    private String following_word;
    private int count;

    DBOutputWritable (String starting_phrase, String following_word, int count) {
        this.starting_phrase = starting_phrase;
        this.following_word = following_word;
        this.count = count;
    }

    @Override
    public void write(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setString(1, starting_phrase);
        preparedStatement.setString(2, following_word);
        preparedStatement.setInt(3, count);
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        this.starting_phrase = resultSet.getString(1);
        this.following_word = resultSet.getString(2);
        this.count = resultSet.getInt(3);
    }
}
