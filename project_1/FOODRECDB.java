package ceng.ceng351.foodrecdb;

import java.sql.*;

public class FOODRECDB implements IFOODRECDB{

    private static String user = "e2448124"; // TODO: Your userName
    private static String password = "2rKfXq2MWGQ9WGK1"; //  TODO: Your password
    private static String host = "momcorp.ceng.metu.edu.tr"; // host name
    private static String database = "db2448124"; // TODO: Your database name
    private static int port = 8080; // port

    private static Connection connection = null;

    @Override
    public void initialize() {

        String url = "jdbc:mysql://" + host + ":" + port + "/" + database;

        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            connection =  DriverManager.getConnection(url, user, password);
        }
        catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }

    }

    @Override
    public int createTables() {

        int numberofTablesInserted = 0;
        String queryCreateMenuItems = "Create Table if not exists MenuItems(" +
                                                                   "itemID int," +
                                                                   "itemName varchar(40)," +
                                                                   "cuisine varchar(20)," +
                                                                   "price int," +
                                                                   "primary key (itemID) )";

        String queryCreateIngredients = "Create Table if not exists Ingredients(" +
                                                                        "ingredientID int ," +
                                                                        "ingredientName varchar(40)," +
                                                                        "primary key (ingredientID) )" ;

        String queryCreateIncludes = "Create Table if not exists Includes (" +
                                                                    "itemID int," +
                                                                    "ingredientID int," +
                                                                    "primary key(itemID, ingredientID)," +
                                                                    "foreign key(itemID) REFERENCES MenuItems(itemID), " +
                                                                    "foreign key(ingredientID) REFERENCES Ingredients(ingredientID) )" ;

        String queryCreateRatings = "Create Table if not exists Ratings (" +
                                                                "ratingID int," +
                                                                "itemID int," +
                                                                "rating int," +
                                                                "ratingDate date," +
                                                                "primary key (ratingID)," +
                                                                "foreign key (itemID) REFERENCES MenuItems(itemID) )" ;

        String queryCreateDietaryCategories = "Create Table if not exists DietaryCategories ( " +
                                                                                    "ingredientID int," +
                                                                                    "dietaryCategory varchar(20)," +
                                                                                    "primary key (ingredientID, dietaryCategory)," +
                                                                                    "foreign key (ingredientID) REFERENCES Ingredients(ingredientID) )" ;

        try {
            Statement statement = connection.createStatement();

            //Player Table
            statement.executeUpdate(queryCreateMenuItems);
            numberofTablesInserted++;

            statement.executeUpdate(queryCreateIngredients);
            numberofTablesInserted++;

            statement.executeUpdate(queryCreateIncludes);
            numberofTablesInserted++;

            statement.executeUpdate(queryCreateRatings);
            numberofTablesInserted++;

            statement.executeUpdate(queryCreateDietaryCategories);
            numberofTablesInserted++;
            //close
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return numberofTablesInserted ;
    }

    @Override
    public int dropTables() {

        int numberofTablesDropped = 0;

        String queryDropMenuItemsTable = "Drop Table if exists MenuItems";
        String queryDropIngredientsTable = "Drop Table if exists Ingredients";
        String queryDropIncludesTable = "Drop Table if exists Includes";
        String queryDropRatingsTable = "Drop Table if exists Ratings";
        String queryDropDietaryCategoriesTable = "Drop Table if exists DietaryCategories";

        try {
            Statement statement = connection.createStatement() ;

            statement.executeUpdate(queryDropIncludesTable);
            numberofTablesDropped++;

            statement.executeUpdate(queryDropRatingsTable);
            numberofTablesDropped++;

            statement.executeUpdate(queryDropDietaryCategoriesTable);
            numberofTablesDropped++;

            statement.executeUpdate(queryDropMenuItemsTable);
            numberofTablesDropped++;

            statement.executeUpdate(queryDropIngredientsTable);
            numberofTablesDropped++;

            //close
            statement.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return numberofTablesDropped;
    }

    @Override
    public int insertMenuItems(MenuItem[] items) {

        int result = 0;

        for(int i = 0; i < items.length; i++ ) {
            String query = "insert into MenuItems values ('" +
                    items[i].getItemID() + "','" +
                    items[i].getItemName() + "','" +
                    items[i].getCuisine() + "','" +
                    items[i].getPrice() + "')";


            try {
                Statement st = connection.createStatement();
                result += st.executeUpdate(query);

                //Close
                st.close();

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    @Override
    public int insertIngredients(Ingredient[] ingredients) {

        int result = 0;

        for(int i = 0; i < ingredients.length; i++ ) {
            String query = "insert into Ingredients values ('" +
                    ingredients[i].getIngredientID() + "','" +
                    ingredients[i].getIngredientName() + "')";

            try {
                Statement st = connection.createStatement();
                result += st.executeUpdate(query);

                //Close
                st.close();

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    @Override
    public int insertIncludes(Includes[] includes) {

        int result = 0;

        for(int i = 0; i < includes.length; i++ ) {
            String query = "insert into Includes values ('" +
                    includes[i].getItemID() + "','" +
                    includes[i].getIngredientID() + "')";
            try {
                Statement st = connection.createStatement();
                result += st.executeUpdate(query);

                //Close
                st.close();

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    @Override
    public int insertDietaryCategories(DietaryCategory[] categories) {

        int result = 0;

        for(int i = 0; i < categories.length; i++ ) {
            String query = "insert into DietaryCategories values ('" +
                    categories[i].getIngredientID() + "','" +
                    categories[i].getDietaryCategory() + "')";

            try {
                Statement st = connection.createStatement();
                result += st.executeUpdate(query);

                //Close
                st.close();

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    @Override
    public int insertRatings(Rating[] ratings) {

        int result = 0;

        for(int i = 0; i < ratings.length; i++ ) {
            String query = "insert into Ratings values ('" +
                    ratings[i].getRatingID() + "','" +
                    ratings[i].getItemID() + "','" +
                    ratings[i].getRating() + "','" +
                    ratings[i].getRatingDate() + "')";


            try {
                Statement st = connection.createStatement();
                result += st.executeUpdate(query);

                //Close
                st.close();

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    @Override
    public MenuItem[] getMenuItemsWithGivenIngredient(String name) {

        ResultSet rs;

        String query = "Select distinct M.itemID, M.itemName, M.cuisine, M.price from MenuItems M, Ingredients I, Includes IC where M.itemID = IC.itemID and " +
                "I.ingredientID = IC.ingredientID and " +
                "I.ingredientName = '" +
                name + "' order by M.itemID ASC;";

        MenuItem[] arr = new MenuItem[0];
        try {
            Statement st = connection.createStatement();
            rs = st.executeQuery(query);

            int size = 0;

            rs.last();    // moves cursor to the last row
            size = rs.getRow(); // get row id
            rs.first();

            arr = new MenuItem[size];
            for (int i = 0; i < size; i++) {
                int id = rs.getInt("itemID");
                String m_name = rs.getString("itemName");
                String cuisine = rs.getString("cuisine");
                int price = rs.getInt("price");
                arr[i] = new MenuItem(id, m_name, cuisine, price);
                rs.next() ;
            }
            //Close
            st.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return arr;
    }

    @Override
    public MenuItem[] getMenuItemsWithoutAnyIngredient() {

        ResultSet rs ;
        String query = "Select distinct M.itemID, M.itemName, M.cuisine, M.price from MenuItems M where M.itemID not in" +
                                    "(Select distinct I.itemID from Includes I)" +
                                    "order by M.itemID ASC" ;

        MenuItem[] arr = new MenuItem[0];
        try {
            Statement st = connection.createStatement();
            rs = st.executeQuery(query);

            int size = 0;

            rs.last();    // moves cursor to the last row
            size = rs.getRow(); // get row id
            rs.first();

            arr = new MenuItem[size];
            for (int i = 0; i < size; i++) {
                int id = rs.getInt("itemID");
                String m_name = rs.getString("itemName");
                String cuisine = rs.getString("cuisine");
                int price = rs.getInt("price");
                arr[i] = new MenuItem(id, m_name, cuisine, price);
                rs.next() ;
            }
            //Close
            st.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return arr;
    }

    @Override
    public Ingredient[] getNotIncludedIngredients() {

        ResultSet rs ;
        String query = "Select distinct I.ingredientID, I.ingredientName from Ingredients I where I.ingredientID not in" +
                "(Select distinct IC.ingredientID from Includes IC)" +
                "order by I.ingredientID ASC" ;


        Ingredient[] arr = new Ingredient[0];
        try {
            Statement st = connection.createStatement();
            rs = st.executeQuery(query);

            int size = 0;

            rs.last();    // moves cursor to the last row
            size = rs.getRow(); // get row id
            rs.first();

            arr = new Ingredient[size];
            for (int i = 0; i < size; i++) {
                int id = rs.getInt("ingredientID");
                String i_name = rs.getString("ingredientName");
                arr[i] = new Ingredient(id, i_name);
                rs.next() ;
            }
            //Close
            st.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return arr;
    }

    @Override
    public MenuItem getMenuItemWithMostIngredients() {

        ResultSet rs ;
        MenuItem mi = null ;
        String query = "Select M.itemID, M.itemName, M.cuisine, M.price from MenuItems M, Includes INC where M.itemID = INC.itemID " +
                "group by M.itemID having count(*) = " +
                "(select max(Temp.ingredientCount) from (Select count(*) as ingredientCount from Includes IC group by IC.itemID) as Temp)" ;

        try {
            Statement st = connection.createStatement();
            rs = st.executeQuery(query);

            rs.first();

            int id = rs.getInt("itemID");
            String m_name = rs.getString("itemName");
            String cuisine = rs.getString("cuisine");
            int price = rs.getInt("price");
            mi = new MenuItem(id, m_name, cuisine, price);

            st.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return mi ;
    }

    @Override
    public QueryResult.MenuItemAverageRatingResult[] getMenuItemsWithAvgRatings() {

        ResultSet rs;
        String query = "(Select M.itemID, M.itemName, AVG(R.rating) as avgrating from MenuItems M, Ratings R where M.itemID = R.itemID and " +
                "R.rating is not null group by M.itemID) union " +
                "(Select distinct M.itemID, M.itemName, null as avgrating from MenuItems M where M.itemID not in" +
                "(Select distinct R.itemID from Ratings R where R.rating is not null)) " +
                "order by avgrating desc";

        QueryResult.MenuItemAverageRatingResult[] arr = new QueryResult.MenuItemAverageRatingResult[0];
        try {
            Statement st = connection.createStatement();
            rs = st.executeQuery(query);

            int size = 0;

            rs.last();    // moves cursor to the last row
            size = rs.getRow(); // get row id
            rs.first();

            arr = new QueryResult.MenuItemAverageRatingResult[size];
            for (int i = 0; i < size; i++) {
                String id = String.valueOf(rs.getInt("itemID"));
                String m_name = rs.getString("itemName");
                String avg = rs.getString("avgrating") ;
                arr[i] = new QueryResult.MenuItemAverageRatingResult(id, m_name, avg);
                rs.next();
            }
            //Close
            st.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return arr;
    }

    @Override
    public MenuItem[] getMenuItemsForDietaryCategory(String category) {

        ResultSet rs ;
        String query = "Select distinct M.itemID, M.itemName, M.cuisine, M.price from MenuItems M, Includes I " +
                        "where M.itemID = I.itemID group by M.itemID having Count(*) =  " +
                        "(Select Count(*) from Includes IC, DietaryCategories DC where " +
                                                            "IC.ingredientID = DC.ingredientID and " +
                                                            "M.itemID = IC.itemID and " +
                                                            "DC.dietaryCategory = '" + category + "' " +
                                                            "group by M.itemID) " +
                        "order by M.itemID asc" ;

        MenuItem[] arr = new MenuItem[0];
        try {
            Statement st = connection.createStatement();
            rs = st.executeQuery(query);

            int size = 0;

            rs.last();    // moves cursor to the last row
            size = rs.getRow(); // get row id
            rs.first();

            arr = new MenuItem[size];
            for (int i = 0; i < size; i++) {
                int id = rs.getInt("itemID");
                String m_name = rs.getString("itemName");
                String cuisine = rs.getString("cuisine");
                int price = rs.getInt("price");
                arr[i] = new MenuItem(id, m_name, cuisine, price);
                rs.next() ;
            }
            //Close
            st.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return arr;
    }

    @Override
    public Ingredient getMostUsedIngredient() {

        ResultSet rs ;
        Ingredient ing = null ;
        String query = "Select I.ingredientID, I.ingredientName from Ingredients I, Includes INC where I.ingredientID = INC.ingredientID " +
                "group by I.ingredientID having count(*) = " +
                "(select max(Temp.number) from (Select count(*) as number from Includes IC group by IC.ingredientID) as Temp)" ;

        try {
            Statement st = connection.createStatement();
            rs = st.executeQuery(query);

            rs.first();

            int id = rs.getInt("ingredientID");
            String i_name = rs.getString("ingredientName");
            ing = new Ingredient(id, i_name);

            st.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return ing ;
    }

    @Override
    public QueryResult.CuisineWithAverageResult[] getCuisinesWithAvgRating() {

        ResultSet rs;
        String query = "(Select M.cuisine, AVG(R.rating) as avgrating from MenuItems M, Ratings R where M.itemID = R.itemID and " +
                "R.rating is not null and R.rating != 0 " +
                "group by M.cuisine) union " +
                "(Select distinct M.cuisine, null as avgrating from MenuItems M where M.cuisine not in " +
                "(Select distinct M1.cuisine from MenuItems M1, Ratings R where M1.itemID = R.itemID and " +
                "R.rating is not null and R.rating != 0)) " +
                "order by avgrating desc " ;


        QueryResult.CuisineWithAverageResult[] arr = new QueryResult.CuisineWithAverageResult[0];
        try {
            Statement st = connection.createStatement();
            rs = st.executeQuery(query);

            int size = 0;

            rs.last();    // moves cursor to the last row
            size = rs.getRow(); // get row id
            rs.first();

            arr = new QueryResult.CuisineWithAverageResult[size];
            for (int i = 0; i < size; i++) {
                String cuisine = rs.getString("cuisine");
                String avg = rs.getString("avgrating") ;
                arr[i] = new QueryResult.CuisineWithAverageResult(cuisine, avg);
                rs.next();
            }
            //Close
            st.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return arr;
    }

    @Override
    public QueryResult.CuisineWithAverageResult[] getCuisinesWithAvgIngredientCount() {

        ResultSet rs;
        String query = "(Select Temp1.cuisine as cuisine, totalIngredients/totalMenus as avgcount from " +
                        "(Select Count(*) as totalIngredients, M.cuisine as cuisine from MenuItems M, Includes IC " +
                        "where M.itemID = IC.itemID group by M.cuisine) as Temp1," +
                        "(Select Count(*) as totalMenus, M.cuisine as cuisine from MenuItems M group by M.cuisine) as Temp2 where " +
                        "Temp1.cuisine = Temp2.cuisine) union " +
                        "(Select M1.cuisine as cuisine, 0 as avgcount from MenuItems M1 where M1.cuisine not in " +
                        "(Select distinct M.cuisine from MenuItems M, Includes IC where M.itemID = IC.itemID))" +
                        "order by avgcount desc" ;


        QueryResult.CuisineWithAverageResult[] arr = new QueryResult.CuisineWithAverageResult[0];
        try {
            Statement st = connection.createStatement();
            rs = st.executeQuery(query);

            int size = 0;

            rs.last();    // moves cursor to the last row
            size = rs.getRow(); // get row id
            rs.first();

            arr = new QueryResult.CuisineWithAverageResult[size];
            for (int i = 0; i < size; i++) {
                String cuisine = rs.getString("cuisine");
                String avg = rs.getString("avgcount") ;
                arr[i] = new QueryResult.CuisineWithAverageResult(cuisine, avg);
                rs.next();
            }
            //Close
            st.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return arr;
    }

    @Override
    public int increasePrice(String ingredientName, String increaseAmount) {

        int result = 0;
        String query = "Update MenuItems Set price = price + " + increaseAmount + " Where itemID in " +
                                                        "(Select distinct IC.itemID from Includes IC, Ingredients I " +
                                                        "where IC.ingredientID = I.ingredientID and I.ingredientName = '" +
                                                        ingredientName + "')" ;


        try {
            Statement st = connection.createStatement();
            result += st.executeUpdate(query);

            //Close
            st.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return result;
    }

    @Override
    public Rating[] deleteOlderRatings(String date) {

        ResultSet rs;
        String findquery = "Select R.ratingID, R.itemID, R.rating, R.ratingDate from Ratings R where " +
                                    "R.ratingDate < '" + date + "'" +
                                    " order by R.ratingID asc" ;
        String deletequery = "Delete from Ratings R where R.ratingDate < '" + date + "'" ;

        Rating[] arr = new Rating[0];
        try {
            Statement st = connection.createStatement();
            rs = st.executeQuery(findquery);

            int size = 0;

            rs.last();    // moves cursor to the last row
            size = rs.getRow(); // get row id
            rs.first();

            arr = new Rating[size];
            for (int i = 0; i < size; i++) {
                int id = rs.getInt("ratingID");
                int m_id = rs.getInt("itemID");
                int rating = rs.getInt("rating");
                String ratingDate = String.valueOf(rs.getDate("ratingDate"));
                arr[i] = new Rating(id, m_id, rating, ratingDate);
                rs.next() ;
            }
            //Close
            st.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            Statement st = connection.createStatement();
            st.executeUpdate(deletequery);

            //Close
            st.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return arr;
    }
}
