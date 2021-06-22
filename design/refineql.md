# RefineQL

RefineQL is a graph-based query language for RefineDB.

*incomplete*

## Examples

```
type Person {
  @primary
  id: string,
  name: string,
}

type Movie {
  @primary
  id: string,
  year: int64,
  director: Person,
  actors: set<Person>,
}

type User {
  @primary
  id: string,
  name: string,
  register_time: int64,
}

type MovieReview {
  @primary
  id: string,
  user_id: string,
  movie_id: string,
  rating: double,
}

export set<Movie> movies;
export set<User> users;
export set<MovieReview> reviews;
```

```
graph average_rating_for_movie(db: schema, movie_id: string, user_id: string): double {
  let movie = db.movies[id == movie_id]!;
  let reviews = db.reviews[movie_id == movie_id];
  fold(reviews, 0.0) {
    (l, r) in
    
  }
}
```