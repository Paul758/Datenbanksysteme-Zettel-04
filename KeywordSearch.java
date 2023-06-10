package org.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class KeywordSearch {
    static Connection db;
    static String keyword;
    static final String QUERY =
            //Part 1
            //Select movies that contain the keyword
            "WITH movieFiltered AS " +
            "(SELECT * " +
            "FROM movie " +
            "WHERE title LIKE ?)," +

            //Get all genres for the movies
            "movieGenres AS " +
            "(SELECT movieFiltered.mid AS movieMid, string_agg(genre.genre, ', ') AS genres " +
            "FROM movieFiltered " +
            "JOIN genre ON movieFiltered.mid = genre.movie_id " +
            "GROUP BY movieFiltered.mid), " +

            //Save all actors and actresses in one relation
            "allActors AS " +
            "(SELECT * FROM actor " +
            "UNION " +
            "SELECT * FROM actress), " +

            //Get the first 5 movie actors that played in the filtered movies
            "movieActors AS " +
            "(SELECT movieID AS movieActorsMid, string_agg(actorNames, '\n\t' ORDER BY actorNames ASC) AS actorName " +
            "FROM" +
                    //Subquery to order all actors of the movie and limit result to 5 actors
                    "(SELECT movieFiltered.mid AS movieID, allActors.name AS actorNames " +
                    "FROM  movieFiltered " +
                    "JOIN allActors ON movieFiltered.mid = allActors.movie_id " +
                    "ORDER BY allActors.name " +
                    "LIMIT 5 ) firstFiveActors " +
            "GROUP BY movieID" +
            ") " +


       "SELECT * FROM movieFiltered " +
                    "JOIN movieGenres ON movieFiltered.mid = movieGenres.movieMid " +
                    "JOIN movieActors ON movieFiltered.mid = movieActors.movieActorsMid " +
                    "ORDER BY movieFiltered.title ASC ";

    static final String queryPartTwo =
            //Part 2
            "WITH allActors AS " +
                    "(SELECT * FROM actor " +
                    "UNION " +
                    "SELECT * FROM actress), " +

            //Get all movies the actors Played In
            "playedIn AS " +
                    "(SELECT allActors.name AS actorName, string_agg(movie.mid, '\n        ') AS moviesPlayedIn " +
                    "FROM allActors " +
                    "JOIN movie ON allActors.movie_id = movie.mid " +
                    "WHERE allActors.name LIKE ? " +
                    "GROUP BY allActors.name) " +

           "SELECT * FROM playedIn";

    static final String queryPartTwoCoStars =
            //Part 2 CoStars
            "WITH allActors AS " +
                    "(SELECT * FROM actor " +
                    "UNION " +
                    "SELECT * FROM actress), " +

                    //Get all movies the actors Played In
                    "playedIn AS " +
                    "(SELECT allActors.name AS actorName, string_agg(movie.mid, '\n        ') AS moviesPlayedIn " +
                    "FROM allActors " +
                    "JOIN movie ON allActors.movie_id = movie.mid " +
                    "WHERE allActors.name LIKE ? " +
                    "GROUP BY allActors.name), " +

                    //Get Co-Stars
                    "coStars AS " +
                    "(SELECT allActors.name AS coStarName, COUNT(allActors.name) AS appearances, playedIn.actorName AS actorKey, " +
                    "ROW_NUMBER() OVER (PARTITION BY playedIn.actorName ORDER BY COUNT(allActors.name) DESC, allActors.name ASC) AS coStarRank " +
                    "FROM allActors " +
                    "JOIN playedIn ON " +
                    "allActors.movie_id = ANY(string_to_array(moviesPlayedIn, '\n')) AND allActors.name <> playedIn.actorName " +
                    "GROUP BY coStarName, actorName)" +

                    "SELECT * FROM coStars WHERE coStarRank <= 5";



    public static void main(String[] args) throws SQLException {
        String database = args[1]; //-d
        String address = args[3]; //-s
        String port = args[5]; //-p
        String USER = args[7]; //-u
        String PASS = args[9]; //-pw
        String KEYWORD = args[11]; //-k
        String DB_URL = "jdbc:postgresql://" + address + ":" + port + "/" + database;  //"jdbc:postgresql://localhost:5432/imdb"
        System.out.println(DB_URL);

        db = DriverManager.getConnection(DB_URL, USER, PASS);

        //Initialize keyword
        keyword = KEYWORD;

        //Setup statement for the first part of the query (Print movies and actors)
        PreparedStatement stmt = db.prepareStatement(QUERY);
        stmt.setString(1, "%" + keyword + "%");
        ResultSet rs = stmt.executeQuery();

        //Setup statement for the second part of the query (Print actor and movies played in)
        PreparedStatement statementPartTwo = db.prepareStatement(queryPartTwo);
        statementPartTwo.setString(1, "%" + keyword + "%");
        ResultSet rsPartTwo = statementPartTwo.executeQuery();

        //Setup statement for the second part of the query (Print co Stars for each actor)
        PreparedStatement statementPartTwoCoStars = db.prepareStatement(queryPartTwoCoStars);
        statementPartTwoCoStars.setString(1, "%" + keyword + "%");
        ResultSet rsPartTwoCoStars = statementPartTwoCoStars.executeQuery();

        //Fill a map with each main actor as a key. Co-stars for each main actor are saved in list as values
        Map<String, ArrayList<String>> coStarMap = new HashMap<>();
        while (rsPartTwoCoStars.next()){
            String key = rsPartTwoCoStars.getString("actorKey");
            String value = rsPartTwoCoStars.getString("coStarName") + " (" + rsPartTwoCoStars.getString("appearances") + ")";

            if(coStarMap.containsKey(key)){
                ArrayList<String> currentList = coStarMap.get(key);
                currentList.add(value);
                coStarMap.put(key, currentList);
            } else {
                ArrayList<String> newList = new ArrayList<>();
                newList.add(value);
                coStarMap.put(key, newList);
            }
        }

        printResults(rs, rsPartTwo, coStarMap);
    }

    private static void printResults(ResultSet rs, ResultSet rsPartTwo, Map<String, ArrayList<String>> coStarMap) throws SQLException {
        //Result
        //Part one: Print all movies
        System.out.println("MOVIES");
        while (rs.next()){
            //Collect movie data
            String title = rs.getString("title");
            String cleanedTitle = title.replaceAll("\\s*\\(\\d{4}\\)", "");
            String year = rs.getString("year");
            String genres = rs.getString("genres");

            //Collect actor names
            String actorNames = rs.getString("actorName");

            //print movie data and actor names
            System.out.println(cleanedTitle + ", " + year + ", " + genres);
            System.out.println("\t" + actorNames);
            System.out.println(" ");
        }

        //Part two: Print all actors
        System.out.println("ACTORS");
        while(rsPartTwo.next()) {
            //Collect actorName and the movies the actor played in
            String actorName = rsPartTwo.getString("actorName");
            String moviesPlayedIn = rsPartTwo.getString("moviesPlayedIn");
            String moviesPlayedInCleaned = moviesPlayedIn.replaceAll("\\s*\\([^)]+\\)", "");

            //Print actorName and movies
            System.out.println(actorName);
            System.out.println("\t PLAYED IN");
            System.out.println("\t \t" + moviesPlayedInCleaned);

            //Print coStars
            System.out.println("\t COSTARS");
            List<String> currentList = coStarMap.get(rsPartTwo.getString("actorName"));
            if(currentList != null){
                for (String coStar : currentList) {
                    System.out.println("\t \t" + coStar);
                };
            }
            System.out.println(" ");
        }

    }
}
