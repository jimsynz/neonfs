# Used by "mix format"
[
  inputs: ["{mix,.formatter}.exs", "{config,lib,test}/**/*.{ex,exs}"],
  plugins: [Absinthe.Formatter, Spark.Formatter],
  import_deps: [:ash_graphql, :absinthe, :ash, :reactor]
]
