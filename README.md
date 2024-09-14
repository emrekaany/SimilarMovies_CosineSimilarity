Overview
This project calculates movie similarity using two different methods:

Cosine similarity based on user ratings
Cosine similarity based on genre information
The final similarity score is a weighted combination of these two methods.

Files Used
u.data: Contains movie rating data with columns (userID, movieID, rating, timestamp).
u.item: Contains movie metadata, including movie ID, movie title, release date, genre information, etc.
Prerequisites
To run this project, you will need:

PySpark installed
A Spark session initiated
MovieLens dataset files (u.data and u.item)
Python 3.x
Code Breakdown
Part 1: Movie Similarity Based on User Ratings
Data Import:

The rating data is imported using a pre-defined schema with columns userID, movieID, rating, and timestamp.
The movie names are also imported from the u.item file, which contains movie metadata such as movie ID and movie title.
Cosine Similarity Calculation:

The rating data is self-joined on the userID to create pairs of ratings for different movies by the same user.
The cosine similarity is calculated using the formula:
similarity
=
∑
(
rating1
×
rating2
)
∑
(
rating1
2
)
×
∑
(
rating2
2
)
similarity= 
∑(rating1 
2
 )
​
 × 
∑(rating2 
2
 )
​
 
∑(rating1×rating2)
​
 
The result is filtered to show only movies with a similarity score greater than 0.97, and at least 50 users rated both movies.
Part 2: Movie Similarity Based on Genre
Data Import:

Movie metadata is imported with the following fields: movie_id, movie_title, release_date, IMDb_URL, and a series of binary genre flags (Action, Adventure, Drama, etc.).
Cosine Similarity Calculation:

The genre data is self-joined on the movie ID to create pairs of genre vectors.
The cosine similarity between genre vectors is calculated in a similar manner as in Part 1, using the dot product of the genre flags and the magnitude of each movie’s genre vector.
Part 3: Combining the Two Similarity Measures
The two similarity scores (one based on ratings, one based on genres) are combined with a weighted sum:
final similarity
=
0.9
×
similarity from ratings
+
0.1
×
similarity from genres
final similarity=0.9×similarity from ratings+0.1×similarity from genres
The results are shown for pairs of movies, including the similarity score, movie names, and other metadata.
Running the Code
Ensure that you have the PySpark environment set up.
Place the dataset files (u.data and u.item) in the appropriate directory.
Modify the file paths in the code as needed to point to your local dataset.
Run the script to compute movie similarity based on user ratings and genres.
Output
The final output consists of pairs of movies, their calculated similarity scores (based on ratings and genres), and their names.
The result is displayed with the top similar movies for a selected target movie.
Customization
You can adjust the weighting (alfa) between ratings-based similarity and genre-based similarity by modifying the combination formula in Part 3.
